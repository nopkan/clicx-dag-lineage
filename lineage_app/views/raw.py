"""A compact table browser for physical Parquet rows and schemas. No JavaScript."""
import json
from urllib.parse import urlencode

from lineage_app.data.raw import LABELS,RawData,cell_text,file_size


def raw_browser(workspace):
    from lineage_app.views.render import e
    w=workspace
    if w.raw_store is None:return '<section class="panel empty">Raw files are unavailable in this workspace.</section>'
    source=RawData(w.raw_store,w.meta)
    name=w.params.get('raw_table','airflow_dag_runs')
    table=source.table(name)
    try:page=int(w.params.get('raw_page','1'))
    except ValueError:page=1
    try:size=int(w.params.get('raw_size','25'))
    except ValueError:size=25
    preview=source.preview(name,w.params.get('raw_q',''),page,size)
    schema=source.schema(name)
    mode='schema' if w.params.get('raw_mode')=='schema' else 'rows'
    def url(**changes):
        values=dict(view='raws',raw_table=name,raw_q=preview['query'],raw_size=preview['size'])
        values.update(changes)
        return '/app?'+e(urlencode({k:v for k,v in values.items() if v not in (None,'')}))
    def download(table,format):return '/raws/download?'+e(urlencode(dict(table=table,format=format)))
    rail=[]
    for t in source.catalog:
        stats=f'{t["rows"]:,} rows' if t['present'] else 'No saved file'
        if t.get('collection',{}).get('state')=='unavailable':stats='Unavailable'
        arrow=f'<a class="raw-quick-download" href="{download(t["name"],"parquet")}" aria-label="Download {e(LABELS[t["name"]])} Parquet" title="Download Parquet">↓</a>' if t['present'] else ''
        rail.append(f'<div class="raw-table-item {"active" if t["name"]==name else ""}"><a class="raw-table-select" href="{url(raw_table=t["name"],raw_q=None,raw_page=None,raw_mode=None)}"><strong>{e(LABELS[t["name"]])}</strong><span class="raw-table-id">{e(t["name"])}</span><small>{stats} · {len(t["fields"])} columns</small></a>{arrow}</div>')
    row_count=f'{table["rows"]:,}' if table['present'] else '—'
    buttons=(f'<a class="button small primary" href="{download(name,"parquet")}" title="Download the full saved table">↓ Parquet</a>' if table['present'] else '<span class="button small raw-disabled" aria-disabled="true">No saved file</span>')
    formats=(('csv','CSV'),('json','JSON'),('schema','Schema JSON')) if table['present'] else (('schema','Schema JSON'),)
    buttons+='<details class="raw-download-menu"><summary class="button small">Download ▾</summary><div>'+''.join(f'<a href="{download(name,key)}">{text}</a>' for key,text in formats)+'</div></details>'
    tabs='<div class="tab-row raw-mode">'+''.join(f'<label><input type="radio" name="raw_mode" value="{key}" form="workspace-sidebar-state" id="raw-mode-{key}"'+(' checked' if mode==key else '')+f'><span>{title}</span></label>' for key,title in (('rows','Rows'),('schema','Schema')))+'</div>'
    headers='<th class="raw-row-number" scope="col">#</th>'+''.join(f'<th scope="col" title="{e(field["type"])}">{e(field["name"])}</th>' for field in table['fields'])
    def cell(value):
        if value is None:return '<span class="raw-null">NULL</span>'
        if value=='':return '<span class="raw-empty">""</span>'
        text=cell_text(value)
        if len(text)>160 or (isinstance(value,str) and value.lstrip().startswith(('{','['))):
            pretty=text
            if isinstance(value,str):
                try:pretty=json.dumps(json.loads(value),ensure_ascii=False,indent=2)
                except (ValueError,TypeError):pass
            return f'<details class="raw-json-cell"><summary title="Expand value">{e(text[:110])}{"…" if len(text)>110 else ""}</summary><pre>{e(pretty)}</pre></details>'
        return e(text)
    rows=[]
    for i,row in enumerate(preview['rows'],preview['start']):
        rows.append(f'<tr><th scope="row" class="raw-row-number">{i:,}</th>'+''.join(f'<td>{cell(row.get(field["name"]))}</td>' for field in table['fields'])+'</tr>')
    data_table='<div class="raw-grid-scroll" tabindex="0" role="region" aria-label="Raw rows"><table class="raw-grid"><caption class="screen-reader">Stored values from the snapshot partition. Timestamp timezones and raw JSON strings are preserved.</caption><thead><tr>'+headers+'</tr></thead><tbody>'+''.join(rows)+'</tbody></table></div>' if rows else '<div class="empty raw-empty-preview">'+('No matching rows.' if preview['query'] else 'This table has no saved rows.')+'</div>'
    previous=f'<a class="button small" href="{url(raw_page=preview["page"]-1)}" aria-label="Previous raw rows">←</a>' if preview['page']>1 else '<span class="button small raw-disabled" aria-disabled="true">←</span>'
    next=f'<a class="button small" href="{url(raw_page=preview["page"]+1)}" aria-label="Next raw rows">→</a>' if preview['page']<preview['pages'] else '<span class="button small raw-disabled" aria-disabled="true">→</span>'
    options=''.join(f'<option value="{n}"'+(' selected' if n==preview['size'] else '')+f'>{n} rows</option>' for n in (25,50,100))
    search=f'<form class="raw-row-search" action="/app" method="get"><input type="hidden" name="view" value="raws"><input type="hidden" name="raw_table" value="{e(name)}"><input name="raw_q" placeholder="Search rows" aria-label="Search raw rows" value="{e(preview["query"])}"><select name="raw_size" aria-label="Raw rows per page">{options}</select><button class="button small">Apply</button>'+ (f'<a class="reset-link" href="{url(raw_q=None,raw_page=None)}">Reset</a>' if preview['query'] else '')+'</form>'
    pagination=f'<div class="raw-pagination"><span>{preview["start"]:,}–{preview["end"]:,} of {preview["count"]:,}'+(' matching rows' if preview['query'] else ' rows')+f'</span><div>{previous}<span>{preview["page"]} / {preview["pages"]}</span>{next}</div></div>'
    schema_rows=''.join(f'<tr><td class="raw-field-index">{i}</td><td class="raw-field-name">{e(f["name"])}</td><td class="raw-type">{e(f["type"])}</td>'+ (f'<td class="raw-type">{e(f.get("model_type") or "—")}</td>' if name=='airflow_monitoring_logs' else '')+f'<td>{"Yes" if f["nullable"] else "No"}</td></tr>' for i,f in enumerate(schema['fields'],1))
    schema_table='<div class="raw-schema-scroll" tabindex="0" role="region" aria-label="Raw table schema"><table class="raw-schema-table"><thead><tr><th>#</th><th>Column</th><th>Stored type</th>'+('<th>Model type</th>' if name=='airflow_monitoring_logs' else '')+'<th>Nullable</th></tr></thead><tbody>'+schema_rows+'</tbody></table></div>'
    notice=''
    if 'collection' in table:
        from lineage_app.views.operations import resource_notice
        from lineage_app.data.operations import TABLES
        key=[k for k,(dataset,_) in TABLES.items() if dataset==name][0]
        notice=resource_notice(w,key)
    return (f'<div class="raw-layout"><aside class="panel raw-tables"><div class="panel-heading"><h2>Tables</h2><span class="count">{len(source.catalog)}</span></div>'+''.join(rail)+'</aside>'
        '<section class="panel raw-preview"><div class="raw-preview-heading"><div><h2>'+e(name)+f'</h2><div class="raw-file-stats"><span>{row_count} rows</span><span>{len(table["fields"])} columns</span><span>{file_size(table["bytes"])}</span></div></div><div class="raw-download-actions">{buttons}</div></div>'
        f'<div class="raw-preview-toolbar">{tabs}<span class="raw-partition" title="{e(table["path"])}">{w.day.isoformat()} · {w.meta["stage"].upper()}</span></div>'
        +notice+f'<div class="raw-rows-view">{search}{data_table}{pagination}</div><div class="raw-schema-view">{schema_table}</div></section></div>')
