import logging
from notebookUtils import *
import os
import pytest
import requests_mock

class TestNotebookUtils():

  def test_notebookSetup(self):
    os.environ['PASSWORD'] = 'pass'
    os.environ['HOSTNAME'] = 'host'
    os.environ['ROOT_DIR'] = '/data'
    c = type('', (), {})()
    c.NotebookApp = type('', (), {})()
    c.Session = type('', (), {})()
    c.FileContentsManager = type('', (), {})()

    result = notebookSetup(c)

    assert c.NotebookApp.ip == 'host'
    assert c.NotebookApp.port == 8888
    assert c.NotebookApp.notebook_dir == '/data/'
    assert c.NotebookApp.allow_root == True
    assert c.NotebookApp.password
    assert c.NotebookApp.base_url == '/data'
    assert c.FileContentsManager.post_save_hook

  def test_scriptPostSave(self):
    with requests_mock.Mocker() as m:
      os.environ['SANDBOXES_API_URL'] = 'http://sandboxes-api'
      os.environ['SANDBOX_ID'] = '123'
      os.environ['DATA_LOADER_API_URL'] = 'dataloader'
      os.environ['KBC_TOKEN'] = 'token'
      dataLoaderMock = m.post('http://dataloader/data-loader-api/save', json={'result': 'ok'})
      apiMock = m.put('http://sandboxes-api/sandboxes/123', json={'result': 'ok'})

      contentsManager = type('', (), {})()
      contentsManager.log = logging
      scriptPostSave({"type": "notebook"}, "/path", contentsManager)

      assert dataLoaderMock.call_count == 1
      assert 'file' in dataLoaderMock.last_request.text
      assert 'tags' in dataLoaderMock.last_request.text

      assert apiMock.call_count == 1
      assert 'lastAutosaveTimestamp' in apiMock.last_request.text
