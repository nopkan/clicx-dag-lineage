"""Download dp_dag_lineage_export folders from S3 with the AWS CLI and the user's SSO profile.

The dashboard never holds AWS credentials: the AWS CLI uses its own SSO cache.
When the sign-in has expired, `aws sso login` opens the browser for approval.
The first page can change the S3 folder, profile and environment per import;
environment variables set the defaults (UAT values shown):

  LINEAGE_S3_URI       s3://uat-sss-dpf-dpt-thl-01-cnsmp/dag_lineage/
  LINEAGE_AWS_PROFILE  nonprod_DP_DataEngineerOS   (needs s3:ListBucket + s3:GetObject)
  LINEAGE_S3_STAGE     uat                          (downloads to data/<stage>/dag_lineage/)
  LINEAGE_AWS_DEVICE_CODE=1                         (Docker: sign in with a link + code, no local browser)

Docker: every browser session gets its own private AWS folder (config + SSO token cache) in the
container's temporary storage, so each person signs in with their own account. When the profile is
missing there, it is written from these settings (put them in .env; they are not credentials):

  LINEAGE_AWS_SSO_START_URL   https://d-xxxxxxxxxx.awsapps.com/start
  LINEAGE_AWS_SSO_REGION      e.g. ap-southeast-1
  LINEAGE_AWS_SSO_ACCOUNT_ID  12-digit AWS account
  LINEAGE_AWS_SSO_ROLE_NAME   permission set the SSO sign-in uses
  LINEAGE_AWS_ROLE_ARN        optional: role the profile then assumes (source_profile chain)
  LINEAGE_AWS_REGION          default region for the profile
"""
import hashlib
import tempfile
import configparser
import os
import re
import shutil
import subprocess
import threading
from pathlib import Path

_local = threading.local()  # .home: this import's private AWS home (Docker), else None


def user_home(session_key):
    """A private AWS home per browser session (Docker only); None means the user's own ~/.aws."""
    if not DEVICE_CODE:
        return None
    digest = hashlib.sha256(str(session_key).encode()).hexdigest()[:24]
    home = Path(os.environ.get('LINEAGE_AWS_USERS_DIR') or Path(tempfile.gettempdir()) / 'lineage-aws') / digest
    (home / '.aws').mkdir(parents=True, exist_ok=True)
    for folder in (home.parent, home, home / '.aws'):
        folder.chmod(0o700)
    return home


def _aws_env():
    env = {**os.environ, 'AWS_PAGER': '', 'AWS_CLI_AUTO_PROMPT': 'off'}
    home = getattr(_local, 'home', None)
    if home:
        env.update(HOME=str(home), AWS_CONFIG_FILE=str(home / '.aws' / 'config'),
                   AWS_SHARED_CREDENTIALS_FILE=str(home / '.aws' / 'credentials'))
    return env

S3_URI = os.environ.get('LINEAGE_S3_URI', 's3://uat-sss-dpf-dpt-thl-01-cnsmp/dag_lineage/')
PROFILE = os.environ.get('LINEAGE_AWS_PROFILE', 'nonprod_DP_DataEngineerOS')
STAGE = os.environ.get('LINEAGE_S3_STAGE', 'uat')
DEVICE_CODE = os.environ.get('LINEAGE_AWS_DEVICE_CODE') == '1'
URL = re.compile(r'https://\S+')
CODE = re.compile(r'\b[A-Z0-9]{4}-[A-Z0-9]{4}\b')


class ImportFailed(RuntimeError):
    pass


STAGES = ('uat', 'dev', 'prd')


def settings(s3_uri=None, profile=None, stage=None):
    """Validated import settings; missing values use the defaults. Raises ValueError."""
    s3_uri = (s3_uri or S3_URI).strip()
    if s3_uri and not s3_uri.endswith('/'):
        s3_uri += '/'
    if not re.fullmatch(r's3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]/(?:[A-Za-z0-9_.=-]+/)*', s3_uri):
        raise ValueError('Enter the S3 folder as s3://bucket/prefix/ (letters, digits and . _ - only).')
    profile = (profile or PROFILE).strip()
    if not re.fullmatch(r'[A-Za-z0-9_.@+-]{1,128}', profile):
        raise ValueError('Enter an AWS profile name from ~/.aws/config.')
    stage = stage or STAGE
    if stage not in STAGES:
        raise ValueError('Choose UAT, DEV or PRD.')
    return dict(s3_uri=s3_uri, profile=profile, stage=stage)


def _aws():
    found = shutil.which('aws') or next((p for p in ('/opt/homebrew/bin/aws', '/usr/local/bin/aws')
                                         if os.access(p, os.X_OK)), None)
    if not found:
        raise ImportFailed('The AWS CLI is not installed where the dashboard runs. Install it with '
                           'brew install awscli, or rebuild the Docker image, which includes it.')
    return found


def _run(args, cancel, timeout, on_line=None):
    """Run one AWS CLI command; returns (exit code, output). Cancel terminates it."""
    try:
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                   text=True, env=_aws_env())
    except OSError:
        raise ImportFailed(f'Cannot start the AWS CLI at {args[0]}.') from None
    lines = []

    def read():
        for line in process.stdout:
            lines.append(line.rstrip())
            if on_line:
                on_line(line.rstrip())
    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    waited = 0.0
    while process.poll() is None:
        if cancel.wait(0.25):
            process.terminate()
            raise RuntimeError('Import cancelled.')
        waited += 0.25
        if waited > timeout:
            process.terminate()
            raise ImportFailed('The AWS CLI did not finish in time. Try Import again.')
    reader.join(2)
    return process.returncode, '\n'.join(lines)


def _signed_in(aws, cancel, profile):
    code, _ = _run([aws, 'sts', 'get-caller-identity', '--profile', profile], cancel, 60)
    return code == 0


def _login_profile(aws, cancel, profile):
    """aws sso login needs the SSO profile at the end of any source_profile chain."""
    for _ in range(5):
        code, output = _run([aws, 'configure', 'get', 'source_profile', '--profile', profile], cancel, 30)
        if code != 0 or not output.strip():
            return profile
        profile = output.strip().splitlines()[-1]
    return profile


def _config_path():
    home = getattr(_local, 'home', None)
    if home:
        return Path(home) / '.aws' / 'config'
    return Path(os.environ.get('AWS_CONFIG_FILE') or Path.home() / '.aws' / 'config')


def _sso_settings():
    keys = dict(start_url='LINEAGE_AWS_SSO_START_URL', sso_region='LINEAGE_AWS_SSO_REGION',
                account_id='LINEAGE_AWS_SSO_ACCOUNT_ID', role_name='LINEAGE_AWS_SSO_ROLE_NAME',
                role_arn='LINEAGE_AWS_ROLE_ARN', region='LINEAGE_AWS_REGION')
    values = {k: os.environ.get(v, '').strip() for k, v in keys.items()}
    return values if all(values[k] for k in ('start_url', 'sso_region', 'account_id', 'role_name')) else None


def _ensure_profile(profile):
    """Docker only: write a missing profile from LINEAGE_AWS_SSO_* into the container's config.

    Returns True when it was added. Never edits a profile that already exists, and never runs
    outside Docker (where ~/.aws/config is the user's own file).
    """
    sso = _sso_settings()
    if not DEVICE_CODE or not sso:
        return False
    path = _config_path()
    config = configparser.RawConfigParser()
    config.read(path)
    section = 'default' if profile == 'default' else f'profile {profile}'
    if config.has_section(section):
        return False
    session, base = 'lineage-sso', f'{profile}-sso'
    if not config.has_section(f'sso-session {session}'):
        config[f'sso-session {session}'] = dict(sso_start_url=sso['start_url'], sso_region=sso['sso_region'],
                                                sso_registration_scopes='sso:account:access')
    sso_profile = dict(sso_session=session, sso_account_id=sso['account_id'], sso_role_name=sso['role_name'])
    if sso['region']:
        sso_profile['region'] = sso['region']
    if sso['role_arn']:
        config[f'profile {base}'] = sso_profile
        config[section] = dict(role_arn=sso['role_arn'], source_profile=base, **({'region': sso['region']} if sso['region'] else {}))
    else:
        config[section] = sso_profile
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as handle:
        config.write(handle)
    path.chmod(0o600)
    return True


def _friendly(output, s3_uri, profile):
    if 'AccessDenied' in output or 'not authorized' in output:
        return f'AWS denied access to {s3_uri} for profile {profile}. It needs s3:ListBucket and s3:GetObject.'
    if 'NoSuchBucket' in output:
        return f'The S3 bucket in {s3_uri} does not exist.'
    if 'could not be found' in output and 'profile' in output:
        where = (' In Docker, set LINEAGE_AWS_SSO_START_URL, _SSO_REGION, _SSO_ACCOUNT_ID and _SSO_ROLE_NAME '
                 'in .env (see .env.example) so the sign-in can start.'
                 if DEVICE_CODE else '')
        return f'AWS profile {profile} is not configured on this machine (~/.aws/config).' + where
    if 'Permission denied' in output or 'Errno 13' in output:
        return ('The download folder is not writable by the dashboard (Permission denied). In Docker, fix it with: '
                'docker exec -u root <container> chown -R lineage:lineage /app/data')
    detail = next((line.strip() for line in reversed(output.splitlines()) if line.strip()), '')
    detail = f' AWS CLI said: {detail[:300]}' if detail else ''
    return 'The AWS CLI could not download the export. Check your network/VPN and try again.' + detail


def download(data_root, cancel, progress, s3_uri=None, profile=None, stage=None, aws_home=None):
    """Sign in if needed, then sync S3 into data/<stage>/dag_lineage/. Returns that folder.

    aws_home: this user's private AWS folder (see user_home); None uses the machine's own ~/.aws.
    """
    _local.home = Path(aws_home) if aws_home else None
    try:
        return _download(data_root, cancel, progress, s3_uri, profile, stage)
    finally:
        _local.home = None


def _download(data_root, cancel, progress, s3_uri, profile, stage):
    try:
        chosen = settings(s3_uri, profile, stage)
    except ValueError as exc:
        raise ImportFailed(str(exc)) from None
    s3_uri, profile, stage = chosen['s3_uri'], chosen['profile'], chosen['stage']
    aws = _aws()
    if _ensure_profile(profile):
        progress(f'Set up AWS profile {profile} for this browser from the LINEAGE_AWS_SSO_* settings.')
    progress(f'Checking AWS sign-in for profile {profile}…')
    if not _signed_in(aws, cancel, profile):
        login = _login_profile(aws, cancel, profile)
        progress('Approve the AWS sign-in that opened in your browser…')
        found = {}

        def show(line):
            for key, pattern in (('url', URL), ('code', CODE)):
                match = pattern.search(line)
                if match and key not in found:
                    found[key] = match[0]
            if 'url' in found:
                extra = f' and enter code {found["code"]}' if 'code' in found and found['code'] not in found['url'] else ''
                progress(f'Sign in to AWS: open {found["url"]} in your browser{extra}, then approve. '
                         'This page continues automatically.')
        # In Docker there is no local browser or localhost callback: use the device-code flow.
        command = [aws, 'sso', 'login', '--profile', login] + (['--use-device-code'] if DEVICE_CODE else [])
        code, output = _run(command, cancel, 600, show)
        if code != 0 or not _signed_in(aws, cancel, profile):
            raise ImportFailed(_friendly(output, s3_uri, profile) if 'profile' in output else
                               'AWS sign-in did not complete. Click Import data to try again.')
    target = Path(data_root) / stage / 'dag_lineage'
    target.mkdir(parents=True, exist_ok=True)
    progress(f'Downloading {s3_uri} …')
    code, output = _run([aws, 's3', 'sync', s3_uri, str(target), '--profile', profile,
                         '--only-show-errors', '--no-progress'], cancel, 1800)
    if code != 0:
        raise ImportFailed(_friendly(output, s3_uri, profile))
    return target
