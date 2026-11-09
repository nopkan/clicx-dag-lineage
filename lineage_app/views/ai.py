"""Native HTML chat, local conversation history and inspectable evidence."""
import re
import secrets
from urllib.parse import urlencode

from markdown_it import MarkdownIt

from lineage_app.data.raw import LABELS
from lineage_app.views.render import e,fmt,icon

MARKDOWN=MarkdownIt('commonmark',{'html':False,'linkify':False}).enable('table').disable('image')
SUGGESTIONS=[('Failed DAGs','Which DAGs have a failed latest run today? Show failed task counts.'),
    ('Slowest tasks','What are the 10 slowest completed tasks today?'),
    ('Failure impact','Which failed tasks have the most downstream tasks marked upstream_failed in the same run?'),
    ('Import errors','Explain the current DAG import errors and what the saved tracebacks show.')]

def answer_html(text,message_id,evidence):
    rendered=MARKDOWN.render(text)
    valid={source['id'] for source in evidence}
    return re.sub(r'\[(E\d+)\]',lambda m:f'<a class="ai-citation" href="#evidence-{message_id}-{m[1]}">{m[1]}</a>' if m[1] in valid else e(m[0]),rendered)

def evidence_html(message,current=True):
    evidence=message['evidence']
    if not evidence:return ''
    cards=[]
    for source in evidence:
        refs=[]
        for table in source['tables']:
            if table=='declared_dag_links':url='/app?view=dependency';label='Declared DAG links'
            else:url='/app?'+e(urlencode(dict(view='raws',raw_table=table)));label=LABELS.get(table,table)
            refs.append(f'<a href="{url}">{e(label)} ↗</a>' if current else f'<span>{e(label)}</span>')
        headers=''.join(f'<th>{e(c)}</th>' for c in source['columns'])
        rows=''.join('<tr>'+''.join(f'<td>{e("NULL" if value is None else str(value))}</td>' for value in row)+'</tr>' for row in source['rows'][:25])
        preview='<div class="ai-evidence-table"><table><thead><tr>'+headers+'</tr></thead><tbody>'+rows+'</tbody></table></div>' if source['rows'] else '<p class="ai-muted">No matching rows.</p>'
        note='<span class="ai-evidence-limit">Limited result</span>' if source.get('truncated') or source.get('clipped') else ''
        if source['returned_rows']>25:note+=f'<span class="ai-evidence-limit">Previewing 25 of {source["returned_rows"]} returned rows</span>'
        query=f'<details class="ai-query"><summary>Query</summary><pre>{e(source["sql"])}</pre></details>' if source.get('sql') else '<p class="ai-muted">'+e(source.get('source_note',''))+'</p>'
        cards.append(f'<article class="ai-evidence-card" id="evidence-{message["id"]}-{e(source["id"])}"><div><strong>{e(source["id"])}</strong><span>{source["returned_rows"]} rows · {e(source["snapshot_date"])}</span>{note}</div><nav>{"".join(refs)}</nav>{preview}{query}</article>')
    return '<details class="ai-evidence"><summary>'+icon('raws')+f'Evidence <span>{len(evidence)}</span></summary>'+''.join(cards)+'</details>'

def chat_state(w):
    return w.ai_state or dict(history=[],chat=None,messages=[],current=True,pending=False,ready=False,model='',catalog=[],error='')

def chat_heading(w):
    state=chat_state(w);chat=state['chat'];chat_id=chat['id'] if chat else ''
    context='<details class="ai-context"><summary>'+icon('raws')+'Context</summary><div><strong>'+e(w.meta['stage'].upper())+' · '+e(chat['snapshot_date'] if chat else w.meta['snapshot_date'])+'</strong><p>Collected '+e(fmt(chat['collected_at'] if chat else w.meta['collected_at']))+'</p><p>Remembers this conversation. Answers are checked against saved data.</p>'
    if state['current']:
        context+='<ul>'+''.join(f'<li>{e(LABELS.get(t["name"],t["name"]))}<span>{str(t["rows"]) if t["present"] else "Not collected"}</span></li>' for t in state['catalog'])+'</ul>'
    else:context+='<p>Earlier snapshot. Start a new chat to ask about the current collection.</p>'
    context+='<small>'+e(state['model'])+' · OpenAI</small></div></details>'
    export=f'<a class="button small" href="/ai/export?chat={chat_id}">↓ Export chat</a>' if chat else ''
    title=e(chat['title'] if chat else 'New conversation')
    return '<div class="ai-page-conversation"><h2 title="'+title+'">'+title+'</h2><span>'+e(w.meta['stage'].upper())+' · '+e(chat['snapshot_date'] if chat else w.meta['snapshot_date'])+' · Bangkok</span></div><div class="ai-page-actions">'+context+export+'</div>'

def chat_view(w):
    state=chat_state(w)
    chat=state['chat'];chat_id=chat['id'] if chat else '';csrf=e(w.csrf)
    def new_button(label='New chat',cls='button small'):
        return f'<form action="/ai/new" method="post"><input type="hidden" name="csrf" value="{csrf}"><button class="{cls}"><span aria-hidden="true">＋</span>{label}</button></form>'
    history=[]
    for item in state['history']:
        active=chat and item['id']==chat_id
        history.append(f'<a class="ai-history-item {"active" if active else ""}" href="/app?view=ai&amp;chat={item["id"]}#ai-latest"><strong>{e(item["title"])}</strong><small>{e(item["snapshot_date"])} · {e(fmt(item["updated_at"],False))}</small></a>')
    rail='<aside class="panel ai-history"><div class="panel-heading"><h2>Conversations</h2>'+new_button('New','button small ai-new-short')+'</div><nav aria-label="Conversations">'+(''.join(history) or '<p class="ai-history-empty">Your conversations appear here.</p>')+'</nav></aside>'
    notice=''
    if not state['ready']:notice='<div class="ai-notice">Add <code>OPENAI_API_KEY</code> to <code>.env</code> to start.</div>'
    if not state['current']:notice+='<div class="ai-notice ai-old-snapshot"><span>Earlier snapshot · conversation is read only</span>'+new_button()+'</div>'
    if state.get('error'):notice+='<div class="ai-error" role="alert">'+e(state['error'])+'</div>'
    body=[]
    for index,message in enumerate(state['messages']):
        if message['role']=='user':
            body.append(f'<article class="ai-message ai-user"><div><span>You</span><time>{e(fmt(message["created_at"],False))}</time></div><p>{e(message["content"])}</p></article>')
            continue
        usage=message.get('usage',{})
        timing=''
        if usage.get('elapsed_ms') is not None and message['state']!='pending':
            seconds=usage['elapsed_ms']/1000
            tooltip=f'{usage.get("requests",0)} OpenAI requests · API {usage.get("api_ms",0)/1000:.1f}s · Local snapshot {usage.get("snapshot_load_ms",0)/1000:.1f}s'
            timing=f'<span class="ai-response-time" title="{e(tooltip)}">{seconds:.1f}s</span>'
        title='<div class="ai-assistant-label">'+icon('ai')+'<span>Terry AI</span>'+timing+'</div>'
        if message['state']=='pending':
            content='<div class="ai-working" role="status"><i></i>Thinking…</div>'
        elif message['state']=='failed':content='<p class="ai-answer-error" role="alert">'+e(message['content'])+'</p>'
        else:content='<div class="ai-answer">'+answer_html(message['content'],message['id'],message['evidence'])+'</div>'+evidence_html(message,state['current'])
        body.append(('<div id="ai-latest"></div>' if index==len(state['messages'])-1 else '')+f'<article class="ai-message ai-assistant" id="answer-{message["id"]}">{title}{content}</article>')
    if not body:
        disabled='' if state['ready'] else ' disabled'
        suggestions=''.join(f'<button name="question" value="{e(question)}"{disabled}>{e(label)}<span>↗</span></button>' for label,question in SUGGESTIONS)
        body.append('<div class="ai-welcome"><div class="ai-welcome-icon">'+icon('ai')+'</div><h2>Ask your data.</h2><p>DAGs, tasks, failures, dependencies.</p><form class="ai-suggestions" action="/ai/ask" method="post"><input type="hidden" name="csrf" value="'+csrf+'"><input type="hidden" name="chat" value="'+chat_id+'"><input type="hidden" name="request_id" value="'+secrets.token_hex(16)+'">'+suggestions+'</form></div>')
    disabled=not state['ready'] or not state['current'] or state['pending']
    disabled_attr=' disabled' if disabled else ''
    # One native text field lets Enter activate the form's arrow submit button.
    composer=f'''<form class="ai-composer" action="/ai/ask" method="post">
      <input type="hidden" name="csrf" value="{csrf}"><input type="hidden" name="chat" value="{chat_id}"><input type="hidden" name="request_id" value="{secrets.token_hex(16)}">
      <label class="screen-reader" for="ai-question">Ask Terry AI about your Airflow data</label>
      <div class="ai-composer-box"><input type="text" id="ai-question" name="question" maxlength="6000" placeholder="Ask Terry AI…" enterkeyhint="send" autocomplete="off" required{disabled_attr}>
      <button class="ai-send" type="submit" aria-label="Send message" title="Send (Enter)"{disabled_attr}>{icon('send')}</button></div>
    </form>'''
    return f'<div class="ai-layout">{rail}<section class="panel ai-chat">{notice}<div class="ai-messages">'+''.join(body)+f'</div>{composer}</section></div>'
