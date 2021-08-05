from datetime import datetime
import json
import os
import sys
from IPython.lib import passwd
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import tempfile


def retrySession(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    """
    Retry http requests on server errors
    Args:
        retries:
        backoff_factor:
        status_forcelist:
        session:
    """
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def saveFile(file_path, sandbox_id, token, log, tags=None):
    """
    Construct a requests POST call with args and kwargs and process the
    results.
    Args:
        file_path: The relative path to the file from the datadir, including filename and extension
        sandbox_id: Id of the sandbox
        token: Keboola Storage token
        log: Logger instance
        tags: Additional tags for the file
    Returns:
        body: Response body parsed from json.
    Raises:
        requests.HTTPError: If the API request fails.
    """

    log.info(f'Attempting to save file {file_path} to Storage')
    if tags is None:
        tags = []
    if 'DATA_LOADER_API_URL' in os.environ and os.environ['DATA_LOADER_API_URL']:
        url = 'http://' + os.environ['DATA_LOADER_API_URL'] + '/data-loader-api/save'
    else:
        url = 'http://data-loader-api/data-loader-api/save'
    headers = {'X-StorageApi-Token': token, 'User-Agent': 'Keboola Sandbox Autosave Request'}
    payload = {'file': {'source': os.path.relpath(file_path), 'tags': ['autosave', 'sandbox-' + sandbox_id] + tags}}

    # the timeout is set to > 3min because of the delay on 400 level exception responses
    # https://keboola.atlassian.net/browse/PS-186
    try:
        r = retrySession().post(url, json=payload, headers=headers, timeout=240)
        r.raise_for_status()
        log.info(f'Successfully saved file {file_path} to Storage')
    except Exception as e:
        log.exception(f'Saving file {file_path} to Storage failed')
        raise e


def updateApiTimestamp(sandbox_id, token, log):
    """
    Update autosave timestamp in Sandboxes API
    Args:
        sandbox_id: Id of the sandbox
        token: Keboola Storage token
        log: Logger instance
    """

    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'Keboola Sandbox Autosave Request',
        'X-StorageApi-Token': token,
    }
    url = os.environ['SANDBOXES_API_URL'] + '/sandboxes/' + sandbox_id
    body = json.dumps({'lastAutosaveTimestamp': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')})
    result = retrySession().patch(url, data=body, headers=headers)
    if result.status_code == requests.codes.ok:
        log.info('Successfully saved autosave to Sandboxes API')
    else:
        log.error('Saving autosave to Sandboxes API errored: ' + result.text)


def getStorageTokenFromEnv(log):
    """
    Find Keboola token in env vars
    Args:
        log: Logger instance
    """

    if 'KBC_TOKEN' in os.environ:
        return os.environ['KBC_TOKEN']
    else:
        log.error('Could not find Keboola Storage API token.')
        raise Exception('Could not find Keboola Storage API token.')


def compressFolder(folder_path):
    """
    Gzip folder
    Args:
        folder_path: Path to the folder
    """
    parent_folder_path = Path(folder_path).parent.absolute()
    gz_path = f'{parent_folder_path}/git_backup.tar.gz'
    os.system(f'cd {parent_folder_path};tar -zcf {gz_path} {Path(folder_path).name}')
    if not os.path.exists(gz_path):
        raise Exception(f'Git folder {parent_folder_path} was not gzipped to {gz_path}')
    return gz_path


def saveFolder(folder_path, sandbox_id, token, log):
    """
    Gzip folder and save it to Keboola Storage
    Args:
        folder_path: Path to the folder
        sandbox_id: Id of the sandbox
        token: Keboola Storage token
        log: Logger instance
    """
    if os.path.exists(folder_path):
        gz_path = compressFolder(folder_path)
        try:
            saveFile(gz_path, sandbox_id, token, log, ['git'])
        finally:
            if os.path.exists(gz_path):
                os.remove(gz_path)


def scriptPostSave(model, os_path, contents_manager, **kwargs):
    """
    Hook on notebook save
    - Saves the notebook file to Keboola Storage
    - Saves .git folder to Keboola Storage if initialized
    - Updates lastAutosaveTimestamp in the API record
    """
    if model['type'] != 'notebook':
        return
    log = contents_manager.log

    sandbox_id = os.environ['SANDBOX_ID']
    token = getStorageTokenFromEnv(log)
    saveFile(os_path, sandbox_id, token, log)
    updateApiTimestamp(sandbox_id, token, log)
    saveFolder('/data/.git', sandbox_id, token, log)


def notebookSetup(c):
    # c is Jupyter config http://jupyter-notebook.readthedocs.io/en/latest/config.html
    print('Initializing Jupyter.', file=sys.stderr)

    if 'HOSTNAME' in os.environ:
        c.ServerApp.ip = os.environ['HOSTNAME']
    else:
        c.ServerApp.ip = '*'
    c.ServerApp.port = 8888
    c.ServerApp.open_browser = False
    # This changes current working dir, so has to be set to /data/
    c.ServerApp.root_dir = '/data/'
    c.Session.debug = False
    # If not set, there is a permission problem with the /data/ directory
    c.ServerApp.allow_root = True

    # Set a password
    if 'PASSWORD' in os.environ and os.environ['PASSWORD']:
        c.ServerApp.token = os.environ['PASSWORD']
        del os.environ['PASSWORD']
    else:
        print('Password must be provided.')
        sys.exit(150)

    if 'ROOT_DIR' in os.environ and os.environ['ROOT_DIR']:
        c.ServerApp.base_url = os.environ['ROOT_DIR']

    c.FileContentsManager.post_save_hook = scriptPostSave
