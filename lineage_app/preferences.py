"""Small, validated browser preferences; no connection or snapshot data."""
import json
import re
from http import cookies
from urllib.parse import quote, unquote

TABS={'overview':'Overview','lineage':'Lineage','dependency':'Dependency',
      'planner':'Schedule',
      'inventory':'DAGs','performance':'Performance','troubleshooting':'Troubleshooting',
      'health':'Health','raws':'Raws','ai':'AI'}
COOKIE='atlas_display'
BADGE_COLORS={'data_platform_color':'#08776d','risk_team_color':'#6550a3'}
# Look-and-feel choices: (default, allowed values). Missing values fall back to the default,
# so older saved cookies keep working.
LOOK={'theme':('light',('light','dark')),
      'accent':('teal',('teal','blue','purple','orange','graphite')),
      'width':('full',('full','centered')),
      'rows':('plain',('plain','striped')),
      'status':('standard',('standard','colorblind'))}
TEXT_SIZES=('small','standard','large','xlarge')

def defaults():
    return dict(tabs=list(TABS),start='auto',density='comfortable',text='standard',catalog=list(TABS),**BADGE_COLORS,
                **{key:default for key,(default,_) in LOOK.items()})

def validate(value):
    if not isinstance(value,dict):raise ValueError('Invalid display settings.')
    tabs=value.get('tabs')
    if not isinstance(tabs,list) or not tabs or any(not isinstance(t,str) or t not in TABS for t in tabs):
        raise ValueError('Keep at least one tab visible.')
    result=dict(tabs=[t for t in TABS if t in tabs],start=value.get('start'),density=value.get('density'),text=value.get('text'),catalog=list(TABS))
    if result['start']!='auto' and result['start'] not in result['tabs']:raise ValueError('Choose a landing page from your visible tabs.')
    if result['density'] not in ('comfortable','compact') or result['text'] not in TEXT_SIZES:
        raise ValueError('Choose a supported display option.')
    for key,(default,allowed) in LOOK.items():
        chosen=value.get(key) or default
        if chosen not in allowed:raise ValueError('Choose a supported display option.')
        result[key]=chosen
    for key,default in BADGE_COLORS.items():
        color=value.get(key,default)
        if not isinstance(color,str) or not re.fullmatch(r'#[0-9a-fA-F]{6}',color):
            raise ValueError('Choose a valid color for each Airflow tag.')
        result[key]=color.lower()
    return result

def badge_palette(color):
    """Keep a chosen hue with a pale background and readable badge text."""
    if not isinstance(color,str) or not re.fullmatch(r'#[0-9a-fA-F]{6}',color):
        raise ValueError('Choose a valid Airflow tag color.')
    rgb=tuple(int(color[i:i+2],16) for i in (1,3,5))
    def luminance(values):
        def channel(c):
            v=c/255
            return v/12.92 if v<=.04045 else ((v+.055)/1.055)**2.4
        return sum(channel(c)*weight for c,weight in zip(values,(.2126,.7152,.0722)))
    def hex_value(values):return '#'+''.join(f'{c:02x}' for c in values)
    background=tuple(round(c*.1+255*.9) for c in rgb)
    foreground=rgb
    while (luminance(background)+.05)/(luminance(foreground)+.05)<4.5:
        foreground=tuple(int(c*.9) for c in foreground)
    border=tuple(round(c*.25+255*.75) for c in foreground)
    return dict(color=hex_value(foreground),background=hex_value(background),border=hex_value(border),accent=color.lower())

def badge_styles(value):
    """Validated CSS variables shared by every source badge in the workspace."""
    styles=[]
    for key,default in BADGE_COLORS.items():
        color=value.get(key,default)
        if color==default:continue
        name=key.removesuffix('_color').replace('_','-')
        for part,hex_color in badge_palette(color).items():
            styles.append(f'--airflow-{name}-{part}:{hex_color}')
    return ';'.join(styles)

def read(header):
    try:
        jar=cookies.SimpleCookie();jar.load(header or '')
        if COOKIE not in jar:return defaults()
        value=json.loads(unquote(jar[COOKIE].value))
        # Old sidebar choices predate Planner; show it as a normal tab.
        # Saving settings records an explicit choice.
        if isinstance(value,dict) and 'catalog' not in value and isinstance(value.get('tabs'),list) and 'planner' not in value['tabs']:
            value['tabs'].append('planner')
        return validate(value)
    except (cookies.CookieError,ValueError,TypeError,KeyError):return defaults()

def cookie(value):
    encoded=quote(json.dumps(validate(value),separators=(',',':')),safe='')
    return f'{COOKIE}={encoded}; Path=/; Max-Age=31536000; HttpOnly; SameSite=Strict'
