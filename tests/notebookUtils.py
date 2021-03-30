import json
import logging
import os
import pytest
import random
import requests_mock
import string
import tempfile

from notebookUtils import getStorageTokenFromEnv, notebookSetup, saveFolder, saveNotebook, scriptPostSave, \
    updateApiTimestamp


class TestNotebookUtils():

    def test_notebookSetup(self):
        os.environ['PASSWORD'] = 'pass'
        os.environ['HOSTNAME'] = 'host'
        os.environ['ROOT_DIR'] = '/data'
        c = type('', (), {})()
        c.NotebookApp = type('', (), {})()
        c.Session = type('', (), {})()
        c.FileContentsManager = type('', (), {})()

        notebookSetup(c)

        assert c.NotebookApp.ip == 'host'
        assert c.NotebookApp.port == 8888
        assert c.NotebookApp.notebook_dir == '/data/'
        assert c.NotebookApp.allow_root is True
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
            scriptPostSave({'type': 'notebook'}, '/path', contentsManager)

            assert dataLoaderMock.call_count == 1
            assert 'file' in dataLoaderMock.last_request.text
            assert 'tags' in dataLoaderMock.last_request.text

            assert apiMock.call_count == 1
            assert 'lastAutosaveTimestamp' in apiMock.last_request.text

    def test_getStorageTokenFromEnvMissing(self):
        os.environ.pop('KBC_TOKEN')
        with pytest.raises(Exception):
            getStorageTokenFromEnv(logging)

    def test_getStorageTokenFromEnvOk(self):
        token = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
        os.environ['KBC_TOKEN'] = token
        assert getStorageTokenFromEnv(logging) == token

    def test_updateApiTimestamp(self):
        with requests_mock.Mocker() as m:
            os.environ['SANDBOXES_API_URL'] = 'http://sandboxes-api'
            apiMock = m.put('http://sandboxes-api/sandboxes/123', json={'result': 'ok'})

            updateApiTimestamp('123', 'token', logging)

            assert apiMock.call_count == 1
            assert 'lastAutosaveTimestamp' in apiMock.last_request.text

    def test_saveNotebook(self):
        with requests_mock.Mocker() as m:
            os.environ['DATA_LOADER_API_URL'] = 'dataloader'
            dataLoaderMock = m.post('http://dataloader/data-loader-api/save', json={'result': 'ok'})

            saveNotebook('/file/path', '123', 'token', logging)

            assert dataLoaderMock.call_count == 1
            response = json.loads(dataLoaderMock.last_request.text)
            assert 'file' in response
            assert 'source' in response['file']
            assert 'tags' in response['file']
            assert 'autosave' in response['file']['tags']
            assert 'sandbox-123' in response['file']['tags']

    def test_saveFolder(self):
        with requests_mock.Mocker() as m:
            os.environ['DATA_LOADER_API_URL'] = 'dataloader'
            dataLoaderMock = m.post('http://dataloader/data-loader-api/save', json={'result': 'ok'})

            folder = tempfile.mkdtemp()
            f = open(folder + '/file.txt', 'a')
            f.write('content')
            f.close()
            saveFolder(folder, '123', 'token', logging)

            assert dataLoaderMock.call_count == 1
            response = json.loads(dataLoaderMock.last_request.text)
            assert 'file' in response
            assert 'source' in response['file']
            assert response['file']['source'] == '/tmp/git_backup.tar.gz'
            assert 'tags' in response['file']
            assert 'autosave' in response['file']['tags']
            assert 'sandbox-123' in response['file']['tags']
