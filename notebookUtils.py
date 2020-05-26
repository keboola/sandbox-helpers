from jupyter_core.paths import jupyter_data_dir
from datetime import datetime
import subprocess
import io
import os
import errno
import stat
import json
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import sys
from notebook.utils import to_api_path



def retrySession(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
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

def saveFile(file_path, token, log):
    """
    Construct a requests POST call with args and kwargs and process the
    results.
    Args:
        file_path: The relative path to the file from the datadir, including filename and extension
        token: keboola storage api token
    Returns:
        body: Response body parsed from json.
    Raises:
        requests.HTTPError: If the API request fails.
    """

    if 'DATA_LOADER_API_URL' in os.environ and os.environ['DATA_LOADER_API_URL']:
        url = 'http://' + os.environ['DATA_LOADER_API_URL'] + '/data-loader-api/save'
    else:
        url = 'http://data-loader-api/data-loader-api/save'
    headers = {'X-StorageApi-Token': token, 'User-Agent': 'Keboola Sandbox Autosave Request'}
    payload = {'file':{'source': file_path, 'tags': ['autosave']}}

    # the timeout is set to > 3min because of the delay on 400 level exception responses
    # https://keboola.atlassian.net/browse/PS-186
    try:
        r = retrySession().post(url, json=payload, headers=headers, timeout=240)
        r.raise_for_status()
    except Exception as e:
        # Handle different error codes
        log.exception("Debugging the error")
        raise e
    else:
        return r.json()

def updateApi(token):
    """
    Update autosave timestamp in Sandboxes API
    """

    headers = { 'X-StorageApi-Token': token, 'User-Agent': 'Keboola Sandbox Autosave Request' }
    url = os.environ['SANDBOXES_API_URL'] + '/sandboxes/' + os.environ['SANDBOX_ID']
    json = { 'lastAutosaveTimestamp': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f') }
    retrySession().put(url, json, headers)

def scriptPostSave(model, os_path, contents_manager, **kwargs):
    """
    saves the ipynb file to keboola storage on every save within the notebook
    """
    if model['type'] != 'notebook':
        return
    log = contents_manager.log

    # get the token from env
    token = None
    if 'KBC_TOKEN' in os.environ:
        token = os.environ['KBC_TOKEN']
    else:
        log.error('Could not find the Keboola Storage API token.')
        raise Exception('Could not find the Keboola Storage API token.')
    try:
        log.info('Attempting to save the file to storage')
        response = saveFile(os.path.relpath(os_path), token, log)
    except requests.HTTPError:
        log.error('Error saving notebook:' + response.json())
        raise

    log.info("Successfully saved the notebook to Keboola Connection")
    updateApi(token)

def notebookSetup(c):
    # c is Jupyter config http://jupyter-notebook.readthedocs.io/en/latest/config.html
    print("Initializing Jupyter.", file=sys.stderr)

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
        from IPython.lib import passwd
        c.NotebookApp.password = passwd(os.environ['PASSWORD'])
        del os.environ['PASSWORD']
    else:
        print('Password must be provided.')
        sys.exit(150)

    if 'ROOT_DIR' in os.environ and os.environ['ROOT_DIR']:
        c.NotebookApp.base_url = os.environ['ROOT_DIR']

    c.FileContentsManager.post_save_hook = scriptPostSave
