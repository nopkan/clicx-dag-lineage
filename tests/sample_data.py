"""Sample Airflow collection for tests, written through the real Parquet storage path."""
from datetime import datetime, timedelta, timezone
from lineage_app.data.model import BKK, map_dag, map_task

def sample_collection(*, collected_at=None, history_days=0):
    now=collected_at or datetime.now(timezone.utc);today=now.astimezone(BKK).date()
    inventory=[];definitions=[];dependencies=[];runs=[];instances=[]
    specs=[
        ('dp_reconcile_mobile_financing_daily','Mobile financing','Consumption','0 2 * * *','failed',118),
        ('dp_load_customer_profile','Customer profile','Curate','0 1 * * *','success',63),
        ('dp_ingest_core_banking','Core banking ingestion','Persist','30 23 * * *','success',44),
        ('dp_outbound_risk_daily','Risk delivery','Outbound','0 5 * * *','success',36),
        ('dp_airflow_monitoring_logs','Platform monitoring','Platform','0 8 * * *','running',12),
        ('dp_load_lending_daily','Lending daily','Consumption','30 2 * * *','success',87),
        ('dp_customer_consent_sync','Consent synchronization','Curate','*/30 * * * *','success',8),
        ('dp_monthly_finance_report','Monthly finance report','Consumption','0 6 1 * *','none',52),
    ]
    upstream={
        specs[0][0]:[specs[1][0],specs[5][0]], specs[1][0]:[specs[2][0]],
        specs[3][0]:[specs[0][0]], specs[5][0]:[specs[2][0]],
        specs[6][0]:[specs[1][0]], specs[7][0]:[specs[0][0]],
    }
    task_ids=['start_pipeline','load_config','persist.define_glue_job','persist.trigger_glue_job',
        'reconcile.define_glue_job','reconcile.trigger_glue_job','validate_results','publish_dataset','end_pipeline']
    pairs=[(0,1),(1,2),(1,4),(2,3),(4,5),(3,6),(5,6),(6,7),(7,8)]
    for ix,(dag_id,name,tag,cron,state,minutes) in enumerate(specs):
        detail=dict(dag_id=dag_id,dag_display_name=name,description=f'{name} pipeline. Collect, transform, validate, and publish platform data.',
            tags=[{'name':tag},{'name':'daily' if ix!=7 else 'monthly'}],owners=['data-platform'],
            timetable_summary=cron,timetable_description='Cron schedule in Asia/Bangkok',timezone='Asia/Bangkok',
            is_paused=ix==7,is_stale=False,catchup=False,has_import_errors=False,max_active_runs=1,max_active_tasks=16)
        detail['tags'].append({'name':'layer:'+('unknown' if tag=='Platform' else tag.lower())})
        detail['tags'].extend({'name':'upstream:'+dag} for dag in upstream.get(dag_id,['none']))
        inventory.append(detail)
        for j,task_id in enumerate(task_ids):
            operator='GlueJobOperator' if 'trigger_glue' in task_id else 'PythonOperator' if j not in (0,8) else 'EmptyOperator'
            definitions.append(dict(dag_id=dag_id,task_id=task_id,task_display_name=task_id.split('.')[-1],
                owner='data-platform',operator_name=operator,trigger_rule='all_success',retries=2,
                depends_on_past=False,wait_for_downstream=False,pool='default_pool',queue='default',
                downstream_task_ids=[task_ids[b] for a,b in pairs if a==j]))
        for a,b in pairs:
            dependencies.append(dict(stage='uat',dag_id=dag_id,snapshot_date=today,
                upstream_task_id=task_ids[a],downstream_task_id=task_ids[b],collected_at=now))
        if ix==7:continue
        for days in range(history_days,-1,-1):
            day=today-timedelta(days=days)
            start=datetime.combine(day,datetime.min.time(),BKK)+timedelta(hours=[2,1,0,5,8,2,4,6][ix],minutes=ix*3)
            factor=1 if days==0 else 0.65+(days%3)*0.06
            finish=start+timedelta(minutes=minutes*factor)
            run_id='scheduled__'+start.astimezone(timezone.utc).isoformat()
            outcome=state if days==0 else 'success'
            raw=dict(dag_id=dag_id,dag_run_id=run_id,run_type='scheduled',state=outcome,
                logical_date=(start-timedelta(days=1)).isoformat(),data_interval_start=(start-timedelta(days=1)).isoformat(),
                data_interval_end=start.isoformat(),run_after=start.isoformat(),queued_at=(start-timedelta(seconds=12)).isoformat(),
                start_date=start.isoformat(),end_date=finish.isoformat() if outcome!='running' else None,triggered_by='timetable')
            run=map_dag('uat',raw,detail,now,'https://airflow.example.com');runs.append(run)
            for j,task_id in enumerate(task_ids):
                t_start=start+timedelta(minutes=minutes*[0,.01,.03,.05,.03,.05,.88,.94,.99][j]*factor)
                length=minutes*[.005,.01,.015,.80,.015,.82,.04,.035,.005][j]*factor
                t_state='success'
                if days==0 and state=='failed':
                    if j==5:t_state='failed'
                    elif j>5:t_state='upstream_failed'
                if days==0 and state=='running':
                    t_state='running' if j in (3,5) else 'scheduled' if j>5 else 'success'
                raw_task=dict(dag_id=dag_id,dag_run_id=run_id,task_id=task_id,map_index=-1,
                    try_number=3 if t_state=='failed' else 1,state=t_state,operator=definitions[-9+j]['operator_name'],
                    start_date=t_start.isoformat() if t_state not in ('scheduled','upstream_failed') else None,
                    end_date=(t_start+timedelta(minutes=length)).isoformat() if t_state in ('success','failed') else None,
                    pool='default_pool',pool_slots=1,queue='default',priority_weight=1,max_tries=2,executor='CeleryExecutor')
                instances.append(map_task(run,raw_task,definitions[-9+j],now))
    from lineage_app.data.operations import TABLES
    failed=next(t for t in instances if t['run_date']==today and t['state']=='failed')
    operational={key:[] for key in TABLES}
    operational['health']=[dict(metadatabase={'status':'healthy'},scheduler={'status':'healthy','latest_scheduler_heartbeat':now.isoformat()},
        triggerer={'status':'healthy','latest_triggerer_heartbeat':now.isoformat()},dag_processor={'status':'healthy','latest_dag_processor_heartbeat':now.isoformat()})]
    operational['diagnostics']=[{**{k:failed[k] for k in ('dag_id','run_id','task_id','map_index','try_number','state')},
        'tries':[dict(try_number=failed['try_number'],map_index=-1,state='failed',start_date=failed['started_at'].isoformat(),
            end_date=failed['completed_at'].isoformat(),duration=failed['duration_seconds'])],
        'tries_status':{'state':'complete'},'log':dict(text='Sample task log: reconciliation validation failed.',truncated=False,try_number=failed['try_number']),
        'log_status':{'state':'complete'}}]
    return dict(collected_at=now,snapshot_date=today,start=today,end=today,
        inventory=inventory,definitions=definitions,dependencies=dependencies,dag_runs=runs,task_runs=instances,
        operations=operational,operational_coverage={k:dict(state='complete',rows=len(v),collected_at=now.isoformat()) for k,v in operational.items()},
        warnings=[],coverage=dict(dags=len(inventory),definitions=len(definitions),dag_runs=len(runs),
            task_instances=len(instances)))
