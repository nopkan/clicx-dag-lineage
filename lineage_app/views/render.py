"""Server-rendered HTML and SVG. All navigation works without JavaScript."""
import html
import re
import textwrap
from collections import defaultdict
from datetime import date, datetime
from string import Template
from urllib.parse import urlencode, quote, urlsplit

from lineage_app.data.model import BKK, graph_layout, job_groups, trace
from lineage_app.paths import PACKAGE_ROOT
from lineage_app.preferences import TABS,badge_styles,defaults

ROOT=PACKAGE_ROOT
TITLES={**TABS,'settings':'Settings'}
def icon(name,extra_class=''):
    return f'<svg class="ui-icon {extra_class}" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"><use href="#ui-{name}"/></svg>'
def e(value):return html.escape(str(value if value is not None else ''),quote=True)
def template(name,**values):return Template((ROOT/name).read_text()).substitute(**values)
def snapshot_label(day,collected_at='',today=None):
    """'Today · as of 15:23', '8 Nov 2026 · as of 23:10', or '5 Nov 2026 · as of 6 Nov 09:12' (Bangkok time)."""
    today=today or datetime.now(BKK).date()
    snapshot=date.fromisoformat(day)
    name='Today' if snapshot==today else 'Yesterday' if (today-snapshot).days==1 else snapshot.strftime('%-d %b %Y')
    try:moment=datetime.fromisoformat(collected_at).astimezone(BKK)
    except (TypeError,ValueError):return name
    when=moment.strftime('%H:%M') if moment.date()==snapshot else moment.strftime('%-d %b %H:%M')
    return f'{name} · as of {when}'
def shell(title,body,refresh=''):
    return template('templates/base.html',title=e(title),body=body,refresh=refresh)
def option(value,label,current):return f'<option value="{e(value)}"'+(' selected' if value==current else '')+f'>{e(label)}</option>'
def human(value):return str(value or '').replace('_',' ').capitalize()
def combined_state(states):
    """Show unfinished or failed children before a successful child."""
    priority=('failed','upstream_failed','running','up_for_retry','restarting',
              'deferred','queued','scheduled',None,'up_for_reschedule','skipped','removed','success')
    return min(states,key=lambda state:priority.index(state) if state in priority else 8) if states else None
def status(value):return f'<span class="state {e(value) if value in ("success","failed","running","queued","scheduled","upstream_failed","up_for_retry") else ""}">{e(human(value) or "No state")}</span>'
def elapsed(seconds):
    if seconds is None:return '—'
    if seconds<60:return f'{seconds:.0f}s'
    if seconds<3600:return f'{seconds/60:.1f}m'
    return f'{seconds/3600:.1f}h'
def fmt(value,with_date=True):
    if not value:return '—'
    if isinstance(value,str):
        try:value=datetime.fromisoformat(value)
        except ValueError:return value
    if value.tzinfo:value=value.astimezone(BKK)
    return value.strftime('%d %b %Y · %H:%M' if with_date else '%H:%M:%S')
def label(d):return d.get('dag_display_name') or d['dag_id']
def tags(d):return [(t['name'] if isinstance(t,dict) else str(t)).strip() for t in (d.get('tags') or [])]
TAG_KEYS=('layer','project','upstream','frequency','team','stakeholder')  # fixed keys offered in the Tag dropdown
def tag_pills(d):return ''.join(f'<span class="tag-pill">{e(t)}</span>' for t in tags(d)[:3])
def airflow_badge(dag,run=None,metadata=None):
    source=dag.get('airflow_source') or (run or {}).get('airflow_source') or (metadata or {}).get('source_name') or '—'
    source={'Our Airflow':'Data Platform','Other Airflow':'Risk Team'}.get(source,source)
    color={'Data Platform':'data-platform','Risk Team':'risk-team'}.get(source,'unknown')
    return f'<span class="airflow-team-badge {color}">{e(source)}</span>'
def safe_link(url):
    return e(url) if urlsplit(url or '').scheme in ('https','http') else '#'

def snapshot_options(choices,selected):
    """<optgroup> per saved date (newest first); each option opens one Airflow source on that date."""
    days=list(dict.fromkeys(c['date'] for c in choices))
    return ''.join(f'<optgroup label="{e(date.fromisoformat(day).strftime("%a %d %b %Y"))}">'
                   +''.join(option(c['key'],c['name']+' · '+c['stage'].upper(),selected) for c in choices if c['date']==day)
                   +'</optgroup>' for day in days)

def connect_page(csrf,error='',settings=None):
    settings=settings or {}
    stage=settings.get('stage','uat')
    body=template('templates/connect.html',csrf=e(csrf),error=f'<div role="alert" class="error-box">{e(error)}</div>' if error else '',
        s3_uri=e(settings.get('s3_uri','')),stages=''.join(option(v,v.upper(),stage) for v in ('uat','dev','prd')))
    return shell('Connect',body)

def progress_page(csrf,job):
    brand='<a class="brand" href="/"><img src="/static/clicx.svg" alt="CLICX" width="122" height="30"><span class="product-name">Lineage</span></a>'
    if job.get('error'):
        saved='<a class="button primary" href="/app">Open saved data</a> ' if job.get('saved') else ''
        body=f'<main class="progress-page">{brand}<h1>{e(job.get("stopped","Collection stopped"))}</h1><div class="error-box">{e(job["error"])}</div>{saved}<a class="button" href="/">Back</a></main>'
        return shell(job.get('stopped','Collection stopped'),body)
    message=job.get('message','Connecting…')
    signin=re.match(r'Sign in to AWS: open (https://\S+) in your browser(?: and enter code ([A-Z0-9]{4}-[A-Z0-9]{4}))?',message)
    if signin:
        # No scripts are allowed on this page, so a plain link opens the sign-in in a new tab.
        code=f'<p class="signin-code-label">Check that AWS shows this code</p><p class="signin-code mono">{e(signin[2])}</p>' if signin[2] else ''
        status=(f'<div class="signin-box" role="status"><a class="button primary signin-button" href="{e(signin[1])}" target="_blank" rel="noopener noreferrer">Open AWS sign-in ↗</a>'
                f'{code}<p class="muted">Approve the request in the new tab. This page continues by itself once you are signed in.</p></div>')
        refresh=4
    else:
        status=f'<p class="status-line" role="status">{e(message)}</p>'
        refresh=2
    body=f'<main class="progress-page">{brand}<h1>{e(job.get("title","Collecting Airflow"))}</h1>{status}<div class="progress-bar"></div><form action="/cancel" method="post"><input type="hidden" name="csrf" value="{e(csrf)}"><button class="button">Cancel</button></form></main>'
    return shell('Collecting',body,f'<meta http-equiv="refresh" content="{refresh};url=/progress">')


class Workspace:
    def __init__(self,data,params,csrf,mode,raw_store=None,sidebar_collapsed=False,ai_state=None,refresh_prompt=None,preferences=None,source_filters=None):
        self.data,self.csrf,self.mode=data,csrf,mode
        self.source_filters=source_filters or dict(airflow='all',airflows=[],dates=[])
        self.raw_store=raw_store
        self.ai_state=ai_state
        self.refresh_prompt=refresh_prompt
        self.sidebar_collapsed=sidebar_collapsed
        self.preferences=preferences or defaults()
        self.params={k:v for k,v in params.items() if k not in ('start','end','baseline','sidebar','_ai_poll','_ai_result','_dag_jump')}
        landing=self.preferences['tabs'][0] if self.preferences['start']=='auto' else self.preferences['start']
        self.meta=data['metadata'];self.view=self.params.get('view',landing)
        if self.view=='timeline':
            self.view='dependency';self.params['view']='dependency'
        if self.view in ('analysis','schema'):
            if self.view=='schema':
                self.params.setdefault('raw_table','airflow_monitoring_logs')
                self.params.setdefault('raw_mode','schema')
            self.view='raws';self.params['view']='raws'
        if self.view not in TITLES:self.view=landing
        if self.view=='dependency' and self.params.get('scope')!='tasks':self.params.setdefault('zoom','1')
        if self.view!='settings':self.params.pop('saved',None)
        if self.view in ('overview','performance','troubleshooting'):self.params.pop('state',None)
        if self.view in ('health','raws','ai','settings','planner'):
            for key in ('q','tag','state'):self.params.pop(key,None)
        self.day=date.fromisoformat(self.meta['snapshot_date'])
        self.rows=[dict(r) for r in data['rows'] if r['run_date']==self.day]
        # Old caches can contain historical benchmarks. Keep the contract, but expose
        # only today's observations in this workspace and its exports.
        for row in self.rows:
            for level in ('dag','task'):
                row[f'{level}_avg_duration_7d_seconds']=None
                row[f'{level}_benchmark_samples']=0 if row[f'{level}_state'] is not None else None
                row[f'{level}_slowdown_pct']=None
                row[f'is_slow_{level}']=False
        self.runs=[r for r in self.rows if r['record_level']=='dag']
        self.tasks=[r for r in self.rows if r['record_level']=='task']
        self.by_dag=defaultdict(list)
        for r in self.runs:self.by_dag[r['dag_id']].append(r)
        for runs in self.by_dag.values():runs.sort(key=lambda r:r['dag_started_at_bkk'] or r['dag_queued_at_bkk'] or datetime.min,reverse=True)
        q=self.params.get('q','').lower().strip();tag=self.params.get('tag','').strip();state=self.params.get('state','')
        tag_q=self.params.get('tag_q','').lower().strip()
        # Tags popup: one chosen value per fixed key (tag_layer=consumption&tag_project=ccd); a DAG must match all of them.
        picked=[f'{k}:{self.params[f"tag_{k}"].strip()}' for k in TAG_KEYS if self.params.get(f'tag_{k}','').strip()]
        if self.view=='dependency' and self.params.get('scope')!='tasks':q=''
        self.inventory=[]
        for d in data['inventory']:
            text=' '.join([d['dag_id'],label(d),*tags(d),*(d.get('owners') or [])])
            latest=(self.by_dag.get(d['dag_id']) or [{}])[0]
            if q and q not in text.lower():continue
            if tag and tag not in tags(d):continue
            if tag_q and not any(tag_q in t.lower() for t in tags(d)):continue
            if picked and not all(p in tags(d) for p in picked):continue
            if state and (latest.get('dag_state') or 'no_runs') != state:continue
            self.inventory.append(d)
        if self.view=='lineage':
            def rail_order(d):
                latest=(self.by_dag.get(d['dag_id']) or [{}])[0].get('dag_state') or 'no_runs'
                priority={'failed':0,'success':1,'running':2,'queued':3,'no_runs':5}
                return (priority.get(latest,4),latest,label(d).casefold(),d['dag_id'].casefold())
            self.inventory.sort(key=rail_order)
        else:
            self.inventory.sort(key=lambda d:(0 if (self.by_dag.get(d['dag_id']) or [{}])[0].get('dag_state')=='failed' else 1,label(d)))
        ids={d['dag_id'] for d in self.inventory}
        self.runs=[r for r in self.runs if r['dag_id'] in ids]
        self.tasks=[r for r in self.tasks if r['dag_id'] in ids]
        dag=self.params.get('dag')
        self.dag=next((d for d in self.inventory if d['dag_id']==dag),self.inventory[0] if self.inventory else None)
        self.dag_id=self.dag['dag_id'] if self.dag else ''
        runs=self.by_dag.get(self.dag_id,[])
        self.run=next((r for r in runs if r['run_id']==self.params.get('run')),runs[0] if runs else None)
        self.definitions=[t for t in data['definitions'] if t['dag_id']==self.dag_id]
        self.edges=sorted({(r['upstream_task_id'],r['downstream_task_id']) for r in data['dependencies']
            if r['dag_id']==self.dag_id and r['upstream_task_id'] and r['downstream_task_id']})
        self.instances=[t for t in self.tasks if self.run and t['dag_id']==self.dag_id and t['run_id']==self.run['run_id']]
        self.task_id=self.params.get('task','')
        if self.params.get('native') and self.task_id:
            groups=job_groups(self.definitions,self.edges)
            expanded=set(self.params.get('expanded','').split(','))
            if any(self.task_id in pair and prefix not in expanded for prefix,pair in groups.items()):
                self.task_id='';self.params.pop('task',None)

    def url(self,**changes):
        params={**self.params,'view':self.view,**changes}
        if changes.get('view') and changes['view']!=self.view:
            params.pop('saved',None)
            if changes['view']=='dependency':params['zoom']='1'
            params.pop('fullscreen',None);params.pop('dependency_fullscreen',None)
            if self.view=='dependency':params.pop('dag_search',None);params.pop('q',None)
            if self.view=='performance':params.pop('perf_run',None);params.pop('perf_task',None)
            if self.view=='planner':
                for key in list(params):
                    if key.startswith('plan_'):params.pop(key,None)
        if changes.get('dag') and changes['dag']!=self.dag_id:
            params.pop('fullscreen',None)
            params.pop('dependency_fullscreen',None)
            if 'expanded' not in changes:params.pop('expanded',None)
        return '/app?'+e(urlencode({k:v for k,v in params.items() if k not in ('start','end','baseline') and v not in (None,'')}))

    def render(self):
        ids={d['dag_id'] for d in self.inventory}
        definitions=[d for d in self.data['definitions'] if d['dag_id'] in ids]
        edges=[d for d in self.data['dependencies'] if d['dag_id'] in ids and d['upstream_task_id']]
        failed=sum(t['task_state']=='failed' for t in self.tasks)
        specs=[('DAGs',len(ids),f'{sum(not d.get("is_paused") for d in self.inventory)} unpaused','▦',''),
            ('Tasks',len(definitions),'current DAG structure','⌘',''),
            ('Dependencies',len(edges),'within collected DAGs','⑂',''),
            ('Failures',failed,'task instances in window','↗','danger' if failed else '')]
        metrics=''.join(f'<div class="metric {c}"><span class="metric-top">{t}</span><span class="metric-value">{v:,}</span></div>' for t,v,n,i,c in specs)
        if self.view!='overview':
            metrics=''
        navigation=''.join(f'<a href="{self.url(view=k,task=None)}" title="{v}" aria-label="{v}" class="{"active" if self.view==k else ""}">{icon(k)}<span>{v}</span></a>' for k,v in TABS.items() if k in self.preferences['tabs'])
        subtitles={'overview':'A connected view of your pipelines, dependencies, and execution health.',
            'lineage':'Trace every task. Follow the dependencies. Find where the flow breaks.',
            'dependency':'Follow declared DAG dependencies across today’s runs.',
            'inventory':'Every accessible DAG, its schedule, ownership, and latest collected execution.',
            'raws':'Browse saved rows, inspect every schema, and download the snapshot.',
            **{key:'' for key in ('performance','troubleshooting','health','ai','settings','planner')}}
        content=getattr(self,'render_'+self.view)()
        page_context=''
        if self.view=='ai':
            from lineage_app.views.ai import chat_heading
            page_context=chat_heading(self)
        # Submit the graph's actual native selection/expansion state when changing
        # the preference. Other pages retain their current query/filter context.
        sidebar_form='workspace-sidebar-state'
        for form_id in ('lineage-state','dependency-state','planner-state'):
            if f'id="{form_id}"' in content:sidebar_form=form_id;break
        if sidebar_form=='workspace-sidebar-state':
            fields={**self.params,'view':self.view}
            if 'id="raw-mode-rows"' in content:fields.pop('raw_mode',None)
            sidebar_state=f'<form id="{sidebar_form}" action="/app" method="get">'+''.join(f'<input type="hidden" name="{e(k)}" value="{e(v)}">' for k,v in fields.items())+'</form>'
        elif sidebar_form=='planner-state':sidebar_state=''
        else:
            sidebar_state=f'<input type="hidden" form="{sidebar_form}" name="zoom" value="{e(self.params.get("zoom","1"))}">'
        # Stale definitions are an expected inventory state, not a failed collection.
        # Classify legacy cached warnings too, without needing another API collection.
        cached_warnings=self.meta.get('warnings',[])
        warnings=[w for w in cached_warnings if not w.startswith('Stale DAG: ')]
        stale_ids=sorted({d['dag_id'] for d in self.data['inventory'] if d.get('is_stale')}
            | {w.removeprefix('Stale DAG: ') for w in cached_warnings if w.startswith('Stale DAG: ')})
        warning_html=f'<details class="warning-box"><summary>Snapshot incomplete · {len(warnings)}</summary>'+''.join(f'<div>{e(w)}</div>' for w in warnings)+'</details>' if warnings else ''
        if stale_ids and self.view in ('overview','inventory','lineage','dependency'):
            stale_label = 'stale DAG' if len(stale_ids)==1 else 'stale DAGs'
            warning_html+=f'<details class="snapshot-notice"><summary>{len(stale_ids)} {stale_label}</summary><p>Task definitions were skipped for DAGs marked stale by Airflow.</p><div class="notice-list">'+''.join(f'<div>{e(dag_id)}</div>' for dag_id in stale_ids)+'</div></details>'
        if self.view=='settings':warning_html=''
        states=option('','All states',self.params.get('state',''))+''.join(option(s,human(s),self.params.get('state','')) for s in ('success','failed','running','queued','no_runs'))
        state_filter='' if self.view in ('overview','performance','troubleshooting') else '<select name="state" aria-label="Filter state">'+states+'</select>'
        all_tags=sorted({t for d in self.data['inventory'] for t in tags(d)})
        tag_controls=self.tag_filter_fields(all_tags)
        global_filters='' if self.view in ('raws','health','ai','settings','planner') else template('templates/filters.html',view=self.view,
            dag=e(self.dag_id),query=e(self.params.get('q','')),state_filter=state_filter,source_filters=self.source_filter_fields(),
            tags=tag_controls)
        if self.view=='dependency' and self.params.get('scope')!='tasks':
            from lineage_app.views.dependency import dependency_search_form
            global_filters=dependency_search_form(self,state_filter,tag_controls)
        top_export='<a class="button small" href="/raws/download-all">↓ Download all</a>' if self.view=='raws' else '<a class="button small" href="/export">↓ Export</a>'
        refresh_prompt=''
        if self.refresh_prompt is not None:
            error=self.refresh_prompt.get('error','')
            credential_fields='<label for="refresh-token">Access token</label><input id="refresh-token" name="token" type="password" placeholder="Paste token" autocomplete="off" spellcheck="false" required autofocus>'
            refresh_prompt=template('templates/refresh.html',csrf=e(self.csrf),return_to=self.url(),stage=e(self.meta['stage'].upper()),credential_fields=credential_fields,
                error=f'<div class="error-box" role="alert">{e(error)}</div>' if error else '')
        prefs=self.preferences
        display_attributes=(f' data-density="{prefs["density"]}" data-text="{prefs["text"]}"' if prefs['density']!='comfortable' or prefs['text']!='standard' else '')
        from lineage_app.preferences import LOOK
        display_attributes+=''.join(f' data-{key}="{e(prefs.get(key,default))}"' for key,(default,_) in LOOK.items() if prefs.get(key,default)!=default)
        colors=badge_styles(self.preferences)
        if colors:display_attributes+=f' style="{e(colors)}"'
        dark=prefs.get('theme')=='dark'
        sun='<svg class="theme-sun" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/></svg>'
        moon='<svg class="theme-moon" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M20.5 14.5A8.5 8.5 0 1 1 9.5 3.5a7 7 0 0 0 11 11Z"/></svg>'
        theme_switch=(f'<form class="theme-form" action="/theme" method="post"><input type="hidden" name="csrf" value="{e(self.csrf)}">'
            f'<input type="hidden" name="return_to" value="{e(self.url())}">'
            f'<button class="theme-switch" name="theme" value="{"light" if dark else "dark"}" role="switch" aria-checked="{"true" if dark else "false"}" '
            f'aria-label="Dark mode" title="Switch to {"light" if dark else "dark"} mode">{sun}{moon}<span class="theme-knob" aria-hidden="true"></span></button></form>')
        body=template('templates/workbench.html',navigation=navigation,global_filters=global_filters,top_export=top_export,cache_mode=self.meta['stage'].upper(),theme_switch=theme_switch,
            display_attributes=display_attributes,
            settings_link=f'<a class="sidebar-link settings-link {"active" if self.view=="settings" else ""}" href="/app?view=settings" title="Settings" aria-label="Settings">{icon("settings")}<span>Settings</span></a>',
            icon_definitions=(ROOT/'static/icons.svg').read_text(),connection_icon=icon('connection'),
            sidebar_collapse_icon=icon('sidebar-collapse','sidebar-collapse-icon'),sidebar_expand_icon=icon('sidebar-expand','sidebar-expand-icon'),
            sidebar_checked='checked' if self.sidebar_collapsed else '',
            sidebar_action='expanded' if self.sidebar_collapsed else 'collapsed',
            sidebar_label='Expand sidebar' if self.sidebar_collapsed else 'Collapse sidebar',
            sidebar_expanded='false' if self.sidebar_collapsed else 'true',
            sidebar_form=sidebar_form,sidebar_state=sidebar_state,
            csrf=e(self.csrf),view_title=TITLES[self.view],view=self.view,refresh_prompt=refresh_prompt,return_to=self.url(),workspace_inert='inert' if self.refresh_prompt is not None else '',
            page_context=page_context,page_heading_class='page-heading ai-page-heading' if self.view=='ai' else 'page-heading',
            source_badge='',
            subtitle=subtitles[self.view],freshness='Updated '+e(fmt(self.meta['collected_at'],False)),
            warnings=warning_html,metrics=metrics,dag=e(self.dag_id),query=e(self.params.get('q','')),state_filter=state_filter,
            tags=tag_controls,
            day_label=e(self.day.strftime('%d %b %Y')+' · '+self.source_label()),content=content,
            cache_path=e('data/'+self.meta['stage']),
            snapshot_date=e(self.meta['snapshot_date']))
        refresh=''
        if self.refresh_prompt is None and self.view=='ai' and self.ai_state and self.ai_state.get('pending'):
            # Navigate once to a held request; completion triggers the response.
            url='/ai/wait?'+urlencode(dict(chat=self.ai_state['chat']['id']))
            refresh=f'<meta http-equiv="refresh" content="0;url={e(url)}">'
        return shell(TITLES[self.view],body,refresh)

    def tag_filter_fields(self,all_tags):
        """A "Tags" button that opens a popup with one value dropdown per fixed key, plus a free-text tag search."""
        rows='';chosen=[]
        for key in TAG_KEYS:
            values=sorted({t.split(':',1)[1] for t in all_tags if ':' in t and t.split(':',1)[0]==key},key=str.lower)
            current=self.params.get(f'tag_{key}','').strip()
            if current:chosen.append(f'{key}: {current}')
            if current and current not in values:values.append(current)
            if not values and not current:continue
            rows+=(f'<label class="tag-picker-row" for="tag-pick-{key}"><span>{e(key)}</span>'
                   f'<select id="tag-pick-{key}" name="tag_{key}">{option("","Any",current)}'+''.join(option(v,v,current) for v in values)+'</select></label>')
        legacy=self.params.get('tag','').strip()
        keep=f'<input type="hidden" name="tag" value="{e(legacy)}">' if legacy else ''
        if legacy:chosen.append(legacy)
        summary=('Tags · '+' · '.join(chosen)) if chosen else 'Tags'
        clear='/app?'+urlencode({k:v for k,v in {**self.params,'view':self.view}.items() if not (k.startswith('tag_') and k!='tag_q') and k!='tag'})
        picker=(f'<details class="tag-picker"><summary class="tag-picker-button{" active" if chosen else ""}" title="{e(summary)}">{e(summary)}</summary>'
                f'<div class="tag-picker-popup" role="group" aria-label="Filter by tag"><h3>Filter by tags</h3><p>Pick a value for any key. DAGs must match every key you set.</p>{rows}'
                f'<div class="tag-picker-actions"><button class="button small primary" type="submit">Apply</button><a class="reset-link" href="{e(clear)}">Clear tags</a></div></div></details>')
        suggestions=''.join(f'<option value="{e(t)}"></option>' for t in all_tags)
        return (keep+picker+
                f'<input class="tag-search" name="tag_q" value="{e(self.params.get("tag_q",""))}" list="tag-search-options" placeholder="Search tags" aria-label="Search tags" autocomplete="off">'
                f'<datalist id="tag-search-options">{suggestions}</datalist>')

    def source_filter_fields(self):
        """Airflow and Date are chosen in Settings only (they still apply to every page)."""
        return ''

    def source_label(self):
        """Which Airflow is shown, for the top bar: 'All Airflows' or one Airflow's name."""
        f=self.source_filters
        return next((name for value,name in f['airflows'] if value==f['airflow']),'All Airflows')

    def render_settings(self):
        from lineage_app.views.settings import settings_page
        return settings_page(self)

    def render_planner(self):
        from lineage_app.views.planner import planner
        return planner(self)

    def render_overview(self):
        rows=[]
        instances=defaultdict(list)
        for task in self.tasks:
            instances[(task['stage'],task['dag_id'],task['run_id'])].append(task)
        for dag in self.inventory:
            latest=(self.by_dag.get(dag['dag_id']) or [None])[0]
            if not latest or latest['dag_state']!='failed':continue
            lineage_url=self.url(view='lineage',dag=dag['dag_id'],task=None,run=latest['run_id'])
            dag_url=(latest.get('dag_run_url') or (dag.get('airflow_url') or self.meta['base_url']).rstrip('/')+'/dags/'+quote(latest['dag_id'],safe='')+'/runs/'+quote(latest['run_id'],safe='')).rstrip('/')+'/'
            tasks=instances[(latest['stage'],latest['dag_id'],latest['run_id'])]
            failures=sum(t['task_state']=='failed' for t in tasks)
            task_counts=f'<span class="failed-task-count">{failures}</span><span class="muted"> / {len(tasks)}</span>'
            badge=airflow_badge(dag,latest,self.meta)
            rows.append(f'<tr><td><a class="dag-name" href="{lineage_url}">{e(label(dag))}</a><div class="dag-id">{e(dag["dag_id"])}</div></td><td class="overview-source">{badge}</td><td>{tag_pills(dag)}</td><td>{status(latest["dag_state"])}</td><td class="mono overview-time">{e(fmt(latest["dag_started_at_bkk"],False))}</td><td class="mono overview-time">{e(fmt(latest["dag_completed_at_bkk"],False))}</td><td class="mono">{e(elapsed(latest["dag_duration_seconds"]))}</td><td>{e(human(latest["run_type"]) or "—")}</td><td class="mono" title="Failed / total collected task instances for this run">{task_counts}</td><td><a class="airflow-dag-link" href="{safe_link(dag_url)}" target="_blank" rel="noopener noreferrer">Open run ↗</a></td><td class="lineage-action"><a class="lineage-row-link" href="{lineage_url}" aria-label="View lineage for {e(label(dag))}" title="View lineage">→</a></td></tr>')
        table='<div class="table-scroll overview-table" tabindex="0" role="region" aria-label="Failed DAGs"><table class="dag-table"><thead><tr><th>DAG</th><th>Airflow</th><th>Tags</th><th>State</th><th>Started at</th><th>Ended at</th><th>Duration</th><th>Run type</th><th>Failed tasks</th><th>DAG run</th><th><span class="screen-reader">Lineage</span></th></tr></thead><tbody>'+''.join(rows)+'</tbody></table></div>' if rows else '<div class="empty">No failed DAGs today.</div>'
        return f'<section class="panel failed-dag-panel"><div class="panel-heading"><h2>Failed DAGs</h2><span class="count">{len(rows)}</span></div>{table}</section>'

    def render_inventory(self):
        from lineage_app.views.inventory import inventory_table
        return inventory_table(self)

    def render_performance(self):
        from lineage_app.views.operations import performance
        return performance(self)

    def render_troubleshooting(self):
        from lineage_app.views.operations import troubleshooting
        return troubleshooting(self)

    def render_health(self):
        from lineage_app.views.operations import health
        return health(self)

    def render_ai(self):
        from lineage_app.views.ai import chat_view
        return chat_view(self)

    def graph_svg(self):
        definitions=list(self.definitions)
        ids={t['task_id'] for t in definitions}
        historical={t['task_id'] for t in self.instances}-ids
        definitions += [dict(task_id=t,operator_name='Not in current definition') for t in sorted(historical)]
        if not definitions:return '<div class="empty"><h3>No task definitions available</h3>This DAG may be stale, unavailable to the token, or have no tasks.</div>',{},{},set()
        _,up,down,cyclic=graph_layout(definitions,self.edges)
        groups=job_groups(definitions,self.edges)
        membership={task:prefix for prefix,pair in groups.items() for task in pair}
        expanded=set(self.params.get('expanded','').split(',')) & groups.keys()
        if self.task_id in membership:expanded.add(membership[self.task_id])
        # Keep a selected child's group open when clicking it again to deselect.
        self.params['expanded']=','.join(sorted(expanded))
        units={task:'group:'+membership[task] if task in membership else task for task in ids|historical}
        unit_edges={(units[a],units[b]) for a,b in self.edges if a in units and b in units and units[a]!=units[b]}
        unit_positions,_,_,_=graph_layout([{'task_id':n} for n in set(units.values())],unit_edges)
        sizes={n:((656,158) if n.startswith('group:') else (280,96)) for n in unit_positions}
        positions={};columns=defaultdict(list);column_x=[]
        for n,(x,y) in unit_positions.items():columns[x].append((y,n))
        x=48
        for col in sorted(columns):
            column_x.append(x);y=84
            for _,n in sorted(columns[col]):
                positions[n]=(x,y);y+=sizes[n][1]+42
            x+=max(sizes[n][0] for _,n in columns[col])+70
        boxes={};visible={}
        for task,unit in units.items():
            ux,uy=positions[unit]
            if task in membership and membership[task] in expanded:
                index=groups[membership[task]].index(task)
                boxes[task]=(ux+16+index*344,uy+46,280,96);visible[task]=task
            else:
                boxes[unit]=(ux,uy,280,96);visible[task]=unit
        full=self.params.get('depth','full')!='direct'
        upstream=trace(self.task_id,up,full);downstream=trace(self.task_id,down,full)
        highlighted=upstream|downstream|{self.task_id}
        try:zoom=min(1.8,max(.55,float(self.params.get('zoom','1'))))
        except ValueError:zoom=1
        width=max(x+sizes[n][0] for n,(x,y) in positions.items())+48
        height=max(y+sizes[n][1] for n,(x,y) in positions.items())+48
        fit_class=' fit' if self.params.get('zoom')=='fit' else ''
        svg=[f'<svg xmlns="http://www.w3.org/2000/svg" class="graph-svg{fit_class}" width="{width*zoom:.0f}" height="{height*zoom:.0f}" viewBox="0 0 {width} {height}" role="group" aria-label="Task lineage for {e(self.dag_id)}"><defs><marker id="arrow" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0 0L7 3.5L0 7Z" fill="#b4bec2"/></marker><marker id="arrow-active" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0 0L7 3.5L0 7Z" fill="#00a99f"/></marker></defs>']
        for i,col in enumerate(column_x,1):
            svg.append(f'<text x="{col}" y="38" font-family="sans-serif" font-size="11" letter-spacing="1" fill="#9aa3aa">STEP {i:02d}</text>')
        for prefix in sorted(expanded):
            x,y=positions['group:'+prefix];w,h=sizes['group:'+prefix]
            collapse=self.url(expanded=','.join(sorted(expanded-{prefix})),task=None if self.task_id in groups[prefix] else self.task_id)+'#job-'+quote(prefix,safe='')
            svg.append(f'<g class="task-group-expanded" id="job-{e(prefix)}"><rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="#f3fbfa" stroke="#b9dcd8"/><a href="{collapse}" aria-label="Collapse {e(prefix)}" aria-expanded="true"><title>{e(prefix)} · Collapse group</title><rect x="{x}" y="{y}" width="{w}" height="40" rx="12" fill="transparent"/><text x="{x+16}" y="{y+26}" font-family="sans-serif" font-size="13" font-weight="600" fill="#252a36">{e(prefix if len(prefix)<72 else prefix[:69]+"…")}</text><text x="{x+w-16}" y="{y+26}" text-anchor="end" font-family="sans-serif" font-size="13" fill="#008f88">−</text></a></g>')
        connectors=defaultdict(list)
        for a,b in self.edges:
            if a not in visible or b not in visible or visible[a]==visible[b]:continue
            connectors[(visible[a],visible[b])].append((a,b))
        for (a,b),originals in sorted(connectors.items()):
            ax,ay,aw,ah=boxes[a];bx,by,bw,bh=boxes[b];ax+=aw;ay+=ah/2;by+=bh/2;mid=(ax+bx)/2
            active=self.task_id and any(s in highlighted and t in highlighted for s,t in originals)
            opacity='0.25' if self.task_id and not active else '1'
            svg.append(f'<path d="M{ax},{ay} C{mid},{ay} {mid},{by} {bx},{by}" fill="none" stroke="{"#00a99f" if active else "#c6cdd0"}" stroke-width="{2 if active else 1.3}" opacity="{opacity}" marker-end="url(#{"arrow-active" if active else "arrow"})"/>')
        by_task=defaultdict(list)
        for t in self.instances:by_task[t['task_id']].append(t)
        for node,(x,y,w,h) in sorted(boxes.items()):
            grouped=node.startswith('group:')
            prefix=node[6:] if grouped else None
            tasks=groups[prefix] if grouped else (node,)
            mapped=[t for task in tasks for t in by_task[task]]
            states=[t['task_state'] for t in mapped]
            if grouped and any(not by_task[task] for task in tasks):states.append(None)
            state=combined_state(states)
            color={'success':'#73965c','failed':'#bd8069','upstream_failed':'#bc9b7b','running':'#729eaf'}.get(state,'#aebaa2')
            selected=not grouped and node==self.task_id
            related=any(task in upstream|downstream for task in tasks)
            outline='#00bfb5' if selected else '#84d8d2' if related else '#dce0e2'
            opacity='0.3' if self.task_id and not any(task in highlighted for task in tasks) else '1'
            if grouped:
                display=prefix;subtitle='2 tasks · Expand +'
                href=self.url(expanded=','.join(sorted(expanded|{prefix})))+'#job-'+quote(prefix,safe='')
                aria=f'Expand {prefix}';extra=' aria-expanded="false"'
            else:
                display=node.rsplit('.',1)[-1];subtitle=node.rsplit('.',1)[0] if '.' in node else self.dag_id
                href=self.url(task=None if selected else node,dag=self.dag_id,run=self.run['run_id'] if self.run else None)
                aria=('Deselect ' if selected else 'Inspect ')+node;extra=''
            title=display
            if len(display)>30:display=display[:28]+'…'
            if len(subtitle)>34:subtitle=subtitle[:32]+'…'
            if grouped:
                lines=textwrap.wrap(prefix,34,break_on_hyphens=False)
                if len(lines)>2:lines[1]=lines[1][:32]+'…'
                name_svg=''.join(f'<text x="{x+15}" y="{y+47+i*17}" font-family="sans-serif" font-size="13" fill="#252a36" font-weight="500">{e(line)}</text>' for i,line in enumerate(lines[:2]))
            else:
                name_svg=f'<text x="{x+15}" y="{y+53}" font-family="sans-serif" font-size="15" fill="#252a36" font-weight="500">{e(display)}</text>'
            group_id=f' id="job-{e(prefix)}"' if grouped else ''
            count=f' · {len(mapped)} maps' if not grouped and len(mapped)>1 else ''
            state_label=human(state) or ('Pending' if mapped else 'No run')
            svg.append(f'<a href="{href}" aria-label="{e(aria)}"{extra}><g{group_id} opacity="{opacity}"><title>{e(title)} · {e(state_label)}</title><rect x="{x}" y="{y+3}" width="280" height="96" rx="10" fill="#252a36" opacity=".18"/><rect x="{x}" y="{y}" width="280" height="96" rx="10" fill="{"#e9fbf9" if selected else "#fff"}" stroke="{outline}" stroke-width="{2 if selected else 1}"/><circle cx="{x+16}" cy="{y+22}" r="3.5" fill="{color}"/><text x="{x+27}" y="{y+26}" font-family="sans-serif" font-size="11" fill="#6e7881">{e(state_label)}{e(count)}</text>{name_svg}<text x="{x+15}" y="{y+(84 if grouped else 77)}" font-family="sans-serif" font-size="11" fill="#89929b">{e(subtitle)}</text><text x="{x+262}" y="{y+25}" font-family="sans-serif" font-size="11" text-anchor="end" fill="{color}">{"+" if grouped else "✓" if state=="success" else "!" if state=="failed" else "•"}</text></g></a>')
        svg.append('</svg>')
        return ''.join(svg),up,down,cyclic

    def render_lineage(self):
        if not self.dag:return '<section class="panel empty"><h3>No matching DAGs</h3>Try another filter.</section>'
        rail=''
        for d in self.inventory:
            run=(self.by_dag.get(d['dag_id']) or [None])[0]
            rail+=f'<a class="dag-rail-item {"active" if d["dag_id"]==self.dag_id else ""}" href="{self.url(dag=d["dag_id"],task=None,run=None,expanded=None,fullscreen=None)}"><strong>{e(label(d))}</strong><div class="rail-source">{airflow_badge(d,run,self.meta)}</div><div class="rail-meta">{status(run["dag_state"]) if run else status("no_runs")}<span>{sum(t["dag_id"]==d["dag_id"] for t in self.data["definitions"])} tasks</span></div></a>'
        run_options=''.join(option(r['run_id'],fmt(r['dag_started_at_bkk'] or r['dag_queued_at_bkk'])+' · '+human(r['dag_state']),self.run['run_id'] if self.run else '') for r in self.by_dag.get(self.dag_id,[]))
        from lineage_app.views.lineage import live_lineage
        svg,detail,state_form=live_lineage(self)
        depth=self.params.get('depth','full')
        try:zoom=float(self.params.get('zoom','1'))
        except ValueError:zoom=1
        if not .55 <= zoom <= 1.8:zoom=1
        zoom_label='Fit' if self.params.get('zoom')=='fit' else f'{zoom:.0%}'
        controls=f'<div class="graph-controls"><select form="lineage-state" name="run" aria-label="Select DAG run">{run_options or "<option>No runs today</option>"}</select><button form="lineage-state" name="zoom" value="{e(self.params.get("zoom","1"))}" class="button small">View</button></div>'
        tabs='<div class="tab-row native-depth">'+''.join(f'<label><input type="radio" form="lineage-state" name="depth" id="trace-{key}" value="{value}"'+(' checked' if (depth=="direct")== (key=="direct") else '')+f'><span>{text}</span></label>' for key,value,text in [('all','full','All levels'),('direct','direct','Direct')])+'</div>'
        buttons=[('−',round(max(.55,zoom-.15),2),'Zoom out'),('+',round(min(1.8,zoom+.15),2),'Zoom in'),('Fit','fit','Fit'),('Reset',1,'Reset')]
        tabs+='<div class="graph-controls">'
        for i,(text,value,aria) in enumerate(buttons):
            if i==1:tabs+=f'<span class="mono">{zoom_label}</span>'
            tabs+=f'<button class="button small" form="lineage-state" name="zoom" value="{value}" aria-label="{aria}">{text}</button>'
        tabs+=f'<button class="button small" form="lineage-state" formaction="/graph.svg" name="zoom" value="{e(self.params.get("zoom","1"))}">↓ SVG</button></div>'
        fullscreen_checked=' checked' if self.params.get('fullscreen')=='1' else ''
        fullscreen=f'<label class="button small lineage-fullscreen" title="Toggle full screen lineage"><input type="checkbox" id="lineage-fullscreen" name="fullscreen" value="1" form="lineage-state" aria-label="Full screen lineage" autocomplete="off"{fullscreen_checked}><svg class="enter-fullscreen" width="18" height="18" viewBox="0 0 18 18" aria-hidden="true"><path d="M6 2H2V6M12 2H16V6M16 12V16H12M6 16H2V12"/></svg><svg class="leave-fullscreen" width="18" height="18" viewBox="0 0 18 18" aria-hidden="true"><path d="M2 6H6V2M12 2V6H16M16 12H12V16M6 16V12H2"/></svg></label>'
        tabs=tabs.removesuffix('</div>')+fullscreen+'</div>'
        _,_,_,cyclic=graph_layout(self.definitions,self.edges)
        notice=''
        if cyclic:notice+='<div class="warning-box">Cycle detected in the current definition. Verify dependency coverage before interpreting this graph.</div>'
        graph=f'<section class="panel lineage-graph"><div class="graph-toolbar"><div class="lineage-heading"><div class="lineage-title"><h2>{e(label(self.dag))}</h2>{airflow_badge(self.dag,self.run,self.meta)}</div><span class="dag-id">{e(self.dag_id)}</span></div>{controls}</div><div class="graph-toolbar">{tabs}</div>{notice}<div class="graph-canvas">{svg}</div><div class="graph-legend"><div class="legend-items"><span><i class="legend-dot"></i>Success</span><span><i class="legend-dot blue"></i>Running</span><span><i class="legend-dot red"></i>Failed</span></div><span>Current task structure</span></div></section>'
        return f'<div class="lineage-layout"><aside class="panel dag-rail"><div class="panel-heading"><h2>DAGs</h2><span class="count">{len(self.inventory)}</span></div>{rail}</aside><div class="graph-area live-lineage">{state_form}{graph}{detail}</div></div>'

    def render_detail(self,up,down,native=False):
        definition=next((d for d in self.definitions if d['task_id']==self.task_id),None)
        instances=[t for t in self.instances if t['task_id']==self.task_id]
        selected=sorted(instances,key=lambda t:(t['map_index'],t['try_number']))[0] if instances else None
        if not definition and not instances:
            items=[('Schedule',self.dag.get('timetable_summary') or 'Unscheduled'),('Timezone',self.dag.get('timezone') or 'Unavailable'),
                ('Owners',', '.join(self.dag.get('owners',[]))),('Max active runs',self.dag.get('max_active_runs')),
                ('Duration',elapsed(self.run['dag_duration_seconds']) if self.run else '—'),
                ('Catchup',str(self.dag.get('catchup'))),('Snapshot',self.meta['snapshot_date']),('Availability','Paused' if self.dag.get('is_paused') else 'Unpaused')]
            title='DAG details';subtitle=''
            fields=self.run
        else:
            definition=definition or {}
            items=[('Operator',selected['task_operator'] if selected else definition.get('operator_name')),
                ('Owner',definition.get('owner')),('Trigger rule',definition.get('trigger_rule')),
                ('Retry policy',definition.get('retries')),('Runtime',elapsed(selected['task_duration_seconds']) if selected else '—'),
                ('Attempt',selected['try_number'] if selected else '—'),
                ('Pool',selected['task_pool'] if selected else definition.get('pool')),('Instances',len(instances))]
            title=self.task_id;subtitle='';fields=selected
        grid='<dl class="detail-grid">'+''.join(f'<div><dt>{e(k)}</dt><dd>{e(v) if v is not None else "—"}</dd></div>' for k,v in items)+'</dl>'
        air_link=f'<a class="button small" href="{safe_link(self.run["dag_run_url"])}" target="_blank" rel="noopener noreferrer">Open Airflow ↗</a>' if self.run else ''
        chips=''
        if self.task_id:
            chips='<div class="lineage-relations">'
            for heading,adj in [('UPSTREAM',up),('DOWNSTREAM',down)]:
                direct=trace(self.task_id,adj,False)
                all_levels=trace(self.task_id,adj)
                related=all_levels if native or self.params.get('depth')!='direct' else direct
                count=lambda n:f'{n:,} '+('task' if n==1 else 'tasks')
                if native:
                    counts=f'<span class="relation-count relation-count-all">{count(len(all_levels))}</span><span class="relation-count relation-count-direct">{count(len(direct))}</span>'
                else:counts=f'<span class="relation-count">{count(len(related))}</span>'
                links=''.join(f'<a class="lineage-relation{" lineage-relation-indirect" if n not in direct else ""}" href="{self.url(task=n)}">{e(n)}</a>' for n in sorted(related))
                chips+=f'<div class="detail-subheading">{heading}{counts}</div><div class="dependency-chips" role="region" tabindex="0" aria-label="{heading.capitalize()} tasks">'+(links or '<span class="dag-id">None</span>')+'</div>'
            chips+='</div>'
        if len(instances)>1:
            chips+='<div class="detail-subheading">MAPPED TASK INSTANCES</div><table class="dag-table"><thead><tr><th>Map index</th><th>State</th><th>Try</th><th>Runtime</th></tr></thead><tbody>'+''.join(f'<tr><td>{t["map_index"]}</td><td>{status(t["task_state"])}</td><td>{t["try_number"]}</td><td>{elapsed(t["task_duration_seconds"])}</td></tr>' for t in instances)+'</tbody></table>'
        field_html=''
        if fields:
            field_html='<details class="details-disclosure"><summary>Fields · '+('map '+str(selected['map_index']) if selected else 'DAG record')+'</summary><table class="fields-table">'+''.join(f'<tr><td>{e(k)}</td><td>{e(v.isoformat() if isinstance(v,(date,datetime)) else v) if v is not None else "<span class=muted>—</span>"}</td></tr>' for k,v in fields.items())+'</table></details>'
        logs=''
        if self.task_id:
            from lineage_app.views.task_logs import log_frame
            logs=log_frame(self,self.task_id)
        return f'<section class="panel node-detail"><div class="detail-top"><div><h3>{e(title)}</h3><span class="dag-id">{e(subtitle)}</span></div>{air_link}</div>{grid}{chips}{logs}{field_html}</section>'

    def render_dependency(self):
        if self.params.get('scope')!='tasks':
            from lineage_app.views.dependency import dependency_timetable
            return dependency_timetable(self)
        from lineage_app.views.timeline import timetable
        return timetable(self)

    def render_timeline(self):
        return self.render_dependency()  # Existing bookmarks and local callers.

    def render_raws(self):
        from lineage_app.views.raw import raw_browser
        return raw_browser(self)
