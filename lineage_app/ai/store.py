"""Local conversation history, isolated by browser owner and snapshot identity."""
import json
import secrets
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime,timezone

from lineage_app.ai.data import clean,snapshot_id

def now():return datetime.now(timezone.utc).isoformat()

class ChatStore:
    def __init__(self,path):
        self.path=path;self.lock=threading.RLock();self.completed=threading.Condition(self.lock)
        path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        with self.db() as db:
            db.executescript('''
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS conversations(
                    id TEXT PRIMARY KEY,owner TEXT NOT NULL,stage TEXT NOT NULL,snapshot_id TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,collected_at TEXT NOT NULL,title TEXT NOT NULL,
                    created_at TEXT NOT NULL,updated_at TEXT NOT NULL,summary TEXT NOT NULL DEFAULT '');
                CREATE TABLE IF NOT EXISTS messages(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,conversation_id TEXT NOT NULL REFERENCES conversations(id),
                    owner TEXT NOT NULL,role TEXT NOT NULL,content TEXT NOT NULL,state TEXT NOT NULL,
                    created_at TEXT NOT NULL,request_id TEXT,progress TEXT NOT NULL DEFAULT '',
                    evidence TEXT NOT NULL DEFAULT '[]',usage TEXT NOT NULL DEFAULT '{}',model TEXT NOT NULL DEFAULT '',
                    UNIQUE(owner,request_id));
                CREATE INDEX IF NOT EXISTS chats_owner ON conversations(owner,stage,updated_at);
                CREATE INDEX IF NOT EXISTS messages_chat ON messages(conversation_id,id);
                CREATE TABLE IF NOT EXISTS ai_events(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id INTEGER NOT NULL REFERENCES messages(id),created_at TEXT NOT NULL,event TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS ai_events_message ON ai_events(message_id,id);
            ''')
            db.execute("UPDATE messages SET state='failed',content='The app restarted before this answer finished. Please ask again.',progress='' WHERE state='pending'")

    @contextmanager
    def db(self):
        with self.lock:
            db=sqlite3.connect(self.path,timeout=5);db.row_factory=sqlite3.Row
            db.execute('PRAGMA foreign_keys=ON')
            try:
                yield db;db.commit()
            except Exception:
                db.rollback();raise
            finally:db.close()

    def _chat(self,db,owner,chat_id):
        row=db.execute('SELECT * FROM conversations WHERE id=? AND owner=?',(chat_id,owner)).fetchone()
        if not row:raise ValueError('Conversation not found.')
        return dict(row)

    def _new(self,db,owner,meta):
        chat_id=secrets.token_hex(16);at=now()
        db.execute('INSERT INTO conversations(id,owner,stage,snapshot_id,snapshot_date,collected_at,title,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)',
            (chat_id,owner,meta['stage'],snapshot_id(meta),meta['snapshot_date'],meta['collected_at'],'New conversation',at,at))
        return chat_id

    def new(self,owner,meta):
        with self.db() as db:return self._new(db,owner,meta)

    def list(self,owner,stage):
        with self.db() as db:return [dict(r) for r in db.execute('SELECT * FROM conversations WHERE owner=? AND stage=? ORDER BY updated_at DESC LIMIT 50',(owner,stage))]

    def page(self,owner,meta,chat_id=''):
        history=self.list(owner,meta['stage'])
        if not chat_id:chat_id=next((c['id'] for c in history if c['snapshot_id']==snapshot_id(meta)),'')
        with self.db() as db:
            chat=self._chat(db,owner,chat_id) if chat_id else None
            if chat and chat['stage']!=meta['stage']:raise ValueError('Open this conversation in its original environment.')
            rows=[dict(r) for r in db.execute('SELECT * FROM messages WHERE conversation_id=? AND owner=? ORDER BY id',(chat_id,owner))] if chat else []
        for row in rows:
            row['evidence']=json.loads(row['evidence']);row['usage']=json.loads(row['usage'])
        return dict(history=history,chat=chat,messages=rows,
            current=not chat or chat['snapshot_id']==snapshot_id(meta),pending=any(r['state']=='pending' for r in rows))

    def enqueue(self,owner,meta,chat_id,question,request_id):
        question=clean(question.strip())
        if not 1<=len(question)<=6000:raise ValueError('Ask a question of up to 6,000 characters.')
        if not isinstance(request_id,str) or len(request_id)!=32 or any(c not in '0123456789abcdef' for c in request_id):raise ValueError('Reload the chat and try again.')
        with self.db() as db:
            existing=db.execute('SELECT conversation_id,id FROM messages WHERE owner=? AND request_id=?',(owner,request_id)).fetchone()
            if existing:return existing['conversation_id'],existing['id'],False
            if db.execute("SELECT 1 FROM messages WHERE owner=? AND state='pending'",(owner,)).fetchone():raise ValueError('An answer is still being prepared. Please wait for it to finish.')
            if not chat_id:chat_id=self._new(db,owner,meta)
            chat=self._chat(db,owner,chat_id)
            if chat['snapshot_id']!=snapshot_id(meta):raise ValueError('This chat uses an earlier snapshot. Start a new chat for current data.')
            at=now()
            db.execute("INSERT INTO messages(conversation_id,owner,role,content,state,created_at) VALUES(?,?,'user',?,'complete',?)",(chat_id,owner,question,at))
            cursor=db.execute("INSERT INTO messages(conversation_id,owner,role,content,state,created_at,request_id,progress) VALUES(?,?,'assistant','','pending',?,?,'Reading the saved snapshot…')",(chat_id,owner,at,request_id))
            title=question[:60]+('…' if len(question)>60 else '') if chat['title']=='New conversation' else chat['title']
            db.execute('UPDATE conversations SET title=?,updated_at=? WHERE id=?',(title,at,chat_id))
            return chat_id,cursor.lastrowid,True

    def turn(self,owner,chat_id,answer_id):
        with self.db() as db:
            chat=self._chat(db,owner,chat_id)
            answer=db.execute("SELECT * FROM messages WHERE id=? AND conversation_id=? AND owner=? AND role='assistant'",(answer_id,chat_id,owner)).fetchone()
            if not answer:raise ValueError('Answer not found.')
            user=db.execute("SELECT * FROM messages WHERE conversation_id=? AND id<? AND role='user' ORDER BY id DESC LIMIT 1",(chat_id,answer_id)).fetchone()
            prior=[dict(r) for r in db.execute("SELECT role,content,id FROM messages WHERE conversation_id=? AND id<? AND state='complete' ORDER BY id",(chat_id,user['id']))]
            recent=prior[-12:];older=prior[:-12]
            notes='Earlier questions (conversation memory, not verified current facts):\n'+'\n'.join('- '+r['content'][:240] for r in older if r['role']=='user')[-5000:] if older else ''
            db.execute('UPDATE conversations SET summary=? WHERE id=?',(notes,chat_id))
            return chat,dict(user),recent,notes

    def search(self,owner,chat_id,before,query,order='newest'):
        if not isinstance(query,str) or len(query.strip())>200 or order not in ('oldest','newest'):raise ValueError('Use a short conversation search phrase and oldest/newest ordering.')
        with self.db() as db:
            self._chat(db,owner,chat_id)
            rows=db.execute("SELECT role,content,created_at FROM messages WHERE conversation_id=? AND owner=? AND id<? AND state='complete' AND instr(lower(content),lower(?))>0 ORDER BY id "+('ASC' if order=='oldest' else 'DESC')+' LIMIT 8',(chat_id,owner,before,query.strip())).fetchall()
        return dict(note='Earlier conversation only. Query snapshot tools to verify data.',messages=[dict(r,content=r['content'][:1800]) for r in rows])

    def progress(self,answer_id,message):
        with self.db() as db:db.execute("UPDATE messages SET progress=? WHERE id=? AND state='pending'",(message,answer_id))

    def wait(self,owner,chat_id,stage,timeout=55):
        """Sleep until completion notification, with no database polling loop."""
        deadline=time.monotonic()+timeout
        with self.completed:
            while True:
                with self.db() as db:
                    chat=self._chat(db,owner,chat_id)
                    if chat['stage']!=stage:raise ValueError('Open this conversation in its original environment.')
                    pending=db.execute("SELECT 1 FROM messages WHERE conversation_id=? AND owner=? AND state='pending' LIMIT 1",(chat_id,owner)).fetchone()
                if not pending:return True
                remaining=deadline-time.monotonic()
                if remaining<=0:return False
                self.completed.wait(remaining)

    def metrics(self,answer_id,usage,event=None):
        """Durable timing logs, including failed requests; never log prompts/data."""
        with self.db() as db:
            db.execute("UPDATE messages SET usage=? WHERE id=? AND state='pending'",(json.dumps(clean(usage)),answer_id))
            if event:db.execute('INSERT INTO ai_events(message_id,created_at,event) VALUES(?,?,?)',(answer_id,now(),json.dumps(clean(event))))

    def finish(self,answer_id,text,evidence,usage,model):
        with self.db() as db:
            usage={**usage,'finished_at':now()}
            db.execute("UPDATE messages SET content=?,state='complete',progress='',evidence=?,usage=?,model=? WHERE id=? AND state='pending'",
                (clean(text),json.dumps(clean(evidence)),json.dumps(usage),model,answer_id))
        with self.completed:self.completed.notify_all()

    def fail(self,answer_id,message):
        with self.db() as db:db.execute("UPDATE messages SET content=?,state='failed',progress='' WHERE id=? AND state='pending'",(message,answer_id))
        with self.completed:self.completed.notify_all()

    def export(self,owner,chat_id):
        with self.db() as db:
            chat=self._chat(db,owner,chat_id)
            rows=[dict(r) for r in db.execute('SELECT role,content,state,created_at,evidence,usage,model FROM messages WHERE conversation_id=? AND owner=? ORDER BY id',(chat_id,owner))]
        for row in rows:row['evidence']=json.loads(row['evidence']);row['usage']=json.loads(row['usage'])
        return dict(title=chat['title'],stage=chat['stage'],snapshot_date=chat['snapshot_date'],collected_at=chat['collected_at'],messages=rows)
