"""Native HTML forms for browser display preferences."""
from lineage_app.preferences import BADGE_COLORS,TABS
from urllib.parse import urlencode
from lineage_app.views.render import airflow_badge,e,icon,option,snapshot_label,template

def saved_data(w):
    """Every locally saved date with its collection time; Open switches the workspace to that date."""
    snapshots=(getattr(w,'source_filters',None) or {}).get('snapshots') or []
    current=w.meta['snapshot_date']
    if not snapshots:
        return '<p class="muted saved-empty">No saved dates yet. Use Import data to download one.</p>'
    rows=[]
    for snap in snapshots:
        label=snapshot_label(snap['date'],snap['collected_at'])
        sources=', '.join(snap['sources'])
        action=('<span class="saved-current">Viewing</span>' if snap['date']==current else
                f'<a class="button small" href="/app?{e(urlencode(dict(view="settings",date=snap["date"])))}">Open</a>')
        rows.append(f'<li class="saved-row{" current" if snap["date"]==current else ""}"><div><strong>{e(label)}</strong>'
                    f'<span class="saved-meta">{e(snap["date"])} · {e(sources)}</span></div>{action}</li>')
    viewing=next((snapshot_label(snap['date'],snap['collected_at']) for snap in snapshots if snap['date']==current),current)
    count=f'{len(snapshots)} date'+('s' if len(snapshots)!=1 else '')
    summary=f'<p class="saved-summary">{e(count)} saved · Viewing <strong>{e(viewing)}</strong></p>'
    # A fixed-height box scrolls on its own, so 100 dates do not make the Settings page long.
    return summary+'<ul class="saved-list" tabindex="0" aria-label="Saved dates">'+''.join(rows)+'</ul>'

def airflow_choice(w):
    """All Airflows or one of them; applies to every page."""
    f=getattr(w,'source_filters',None) or {}
    choices=[('all','All Airflows')]+list(f.get('airflows') or [])
    current=f.get('airflow','all')
    items=''.join(f'<span class="saved-current">{e(name)}</span>' if value==current else
                  f'<a class="button small" href="/app?{e(urlencode(dict(view="settings",airflow=value)))}">{e(name)}</a>'
                  for value,name in choices)
    return f'<div class="source-choice" role="group" aria-label="Airflow">{items}</div>'

def settings_page(w):
    prefs=getattr(w,'settings_draft',w.preferences)
    tabs=prefs.get('tabs',[])
    choices=''.join(
        f'<label class="settings-tab">{icon(key)}<span>{title}</span>'
        f'<input type="checkbox" name="tabs" value="{key}"'+(' checked' if key in tabs else '')+'></label>'
        for key,title in TABS.items())
    error=getattr(w,'settings_error','')
    return template('templates/settings.html',csrf=e(w.csrf),tabs=choices,saved_data=saved_data(w),airflow_choice=airflow_choice(w),
        data_platform_badge=airflow_badge({'airflow_source':'Data Platform'}),risk_team_badge=airflow_badge({'airflow_source':'Risk Team'}),
        data_platform_color=e(prefs.get('data_platform_color',BADGE_COLORS['data_platform_color'])),
        risk_team_color=e(prefs.get('risk_team_color',BADGE_COLORS['risk_team_color'])),
        notice=f'<div class="settings-saved" role="status">Settings saved</div>' if w.params.get('saved')=='1' else '',
        error=f'<div class="error-box" role="alert">{e(error)}</div>' if error else '',
        start=option('auto','First visible tab',prefs.get('start'))+''.join(option(k,v,prefs.get('start')) for k,v in TABS.items()),
        density=''.join(option(k,v,prefs.get('density')) for k,v in [('comfortable','Comfortable'),('compact','Compact')]),
        text=''.join(option(k,v,prefs.get('text')) for k,v in [('small','Small'),('standard','Standard'),('large','Large'),('xlarge','Extra large')]),
        theme=''.join(option(k,v,prefs.get('theme','light')) for k,v in [('light','Light'),('dark','Dark')]),
        accent=''.join(option(k,v,prefs.get('accent','teal')) for k,v in [('teal','Teal'),('blue','Blue'),('purple','Purple'),('orange','Orange'),('graphite','Graphite')]),
        width=''.join(option(k,v,prefs.get('width','full')) for k,v in [('full','Full width'),('centered','Centred')]),
        rows=''.join(option(k,v,prefs.get('rows','plain')) for k,v in [('plain','Plain'),('striped','Striped')]),
        status=''.join(option(k,v,prefs.get('status','standard')) for k,v in [('standard','Standard (green / red)'),('colorblind','Colour-blind friendly (blue / orange)')]))
