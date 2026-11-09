"""Airflow source identity: canonical base URL → SHA-256 source ID → storage key."""
import hashlib
from urllib.parse import urlsplit, urlunsplit

STAGES = ('uat', 'dev', 'prd')

def canonical_url(value):
    if not isinstance(value, str) or not value.strip() or len(value)>1024 or any(ord(c)<33 for c in value.strip()):
        raise ValueError('Enter a valid Airflow base URL.')
    try:
        p=urlsplit(value.strip())
        if p.scheme not in ('https','http') or not p.hostname or p.username is not None or p.password is not None or p.query or p.fragment:
            raise ValueError()
        port=p.port
        host=p.hostname.lower()
        if ':' in host:host='['+host+']'
        authority=host+(':'+str(port) if port and (p.scheme,port) not in (('http',80),('https',443)) else '')
        path=p.path.rstrip('/')
        if path.endswith(('/api/v2','/auth/token')) or '\\' in value:raise ValueError()
        return urlunsplit((p.scheme.lower(),authority,path,'',''))
    except ValueError:
        raise ValueError('Enter an Airflow base URL without credentials, API paths, query or fragment.') from None

def source_id(base):return hashlib.sha256(canonical_url(base).encode()).hexdigest()
def cache_key(stage,base):return stage+':'+source_id(base)
