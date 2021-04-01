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

    if tags is None:
        tags = []
    if 'DATA_LOADER_API_URL' in os.environ and os.environ['DATA_LOADER_API_URL']:
        url = 'http://' + os.environ['DATA_LOADER_API_URL'] + '/data-loader-api/save'
    else:
        url = 'http://data-loader-api/data-loader-api/save'
    headers = {'X-StorageApi-Token': token, 'User-Agent': 'Keboola Sandbox Autosave Request'}
    payload = {'file': {'source': file_path, 'tags': ['autosave', 'sandbox-' + sandbox_id] + tags}}

    # the timeout is set to > 3min because of the delay on 400 level exception responses
    # https://keboola.atlassian.net/browse/PS-186
    try:
        r = retrySession().post(url, json=payload, headers=headers, timeout=240)
        r.raise_for_status()
    except Exception as e:
        # Handle different error codes
        log.exception('Debugging the error')
        raise e
    else:
        return r.json()


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
    result = retrySession().put(url, data=body, headers=headers)
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


def saveNotebook(file, sandbox_id, token, log):
    """
    Save notebook file to Keboola Storage
    Args:
        file: Path to the notebook file
        sandbox_id: Id of the sandbox
        token: Keboola Storage token
        log: Logger instance
    """
    response = None
    try:
        log.info('Attempting to save the file to storage')
        response = saveFile(os.path.relpath(file), sandbox_id, token, log)
        log.info('Successfully saved the notebook to Keboola Storage')
    except requests.HTTPError as err:
        message = 'Error saving notebook.'
        if response:
            message += ' ' + response.json()
        message += ' {0}'.format(err)
        log.error(message)
        raise


def saveFolder(folder_path, sandbox_id, token, log):
    """
    Gzip folder and save it to Keboola Storage
    Args:
        folder_path: Path to the folder
        sandbox_id: Id of the sandbox
        token: Keboola Storage token
        log: Logger instance
    """
    gz_path = f'{tempfile.mkdtemp()}/git_backup.tar.gz'
    if os.path.exists(folder_path):
        parent_folder_path = Path(folder_path).parent.absolute()
        os.system(f'cd {parent_folder_path};tar -zcf {gz_path} {Path(folder_path).name}')
        if not os.path.exists(gz_path):
            log.error('Git folder was not gzipped')
        else:
            response = None
            try:
                response = saveFile(os.path.relpath(gz_path), sandbox_id, token, log, ['git'])
                log.info('Successfully saved git folder to Keboola Storage')
            except requests.HTTPError as err:
                message = 'Error saving gzipped git folder.'
                if response:
                    message += ' ' + response.json()
                message += ' {0}'.format(err)
                log.error(message)
                raise


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
    saveNotebook(os_path, sandbox_id, token, log)
    updateApiTimestamp(sandbox_id, token, log)
    saveFolder('/data/.git', sandbox_id, token, log)


def notebookSetup(c):
    # c is Jupyter config http://jupyter-notebook.readthedocs.io/en/latest/config.html
    print('Initializing Jupyter.', file=sys.stderr)

    if 'HOSTNAME' in os.environ:
        c.NotebookApp.ip = os.environ['HOSTNAME']
    else:
        c.NotebookApp.ip = '*'
    c.NotebookApp.port = 8888
    c.NotebookApp.open_browser = False
    # This changes current working dir, so has to be set to /data/
    c.NotebookApp.notebook_dir = '/data/'
    c.Session.debug = False
    # If not set, there is a permission problem with the /data/ directory
    c.NotebookApp.allow_root = True

    # Set a password
    if 'PASSWORD' in os.environ and os.environ['PASSWORD']:
        c.NotebookApp.password = passwd(os.environ['PASSWORD'])
        del os.environ['PASSWORD']
    else:
        print('Password must be provided.')
        sys.exit(150)

    if 'ROOT_DIR' in os.environ and os.environ['ROOT_DIR']:
        c.NotebookApp.base_url = os.environ['ROOT_DIR']

    c.FileContentsManager.post_save_hook = scriptPostSave
