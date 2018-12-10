import pytest

from carcosa.cluster import ClusterClient
from carcosa.qsystems.local import LocalServer

def test_no_uri():
    c = ClusterClient()
    assert c.uri == None
    with pytest.raises(ValueError):
        c.server

def test_uri():
    uri = 'foo'
    c = ClusterClient(uri=uri)
    assert c.uri == uri

    c = ClusterClient()
    c.uri = uri
    assert c.uri == uri

    with pytest.raises(TypeError):
        c.uri = 10
    assert uri == uri

def test_connected():
    c = ClusterClient()
    assert not c.connected
    c._server = 'something'
    assert c.connected

def test_remote_path():
    rpath = '/tmp'
    c = ClusterClient(remote_path=rpath)
    assert c.remote_path == rpath

    c = ClusterClient()
    assert c.remote_path is None
    c.remote_path = rpath
    assert c.remote_path == rpath

def test_local_path():
    lpath = '/tmp'
    c = ClusterClient(local_path=lpath)
    assert c.local_path == lpath

    c = ClusterClient()
    with pytest.raises(ValueError):
        c.local_path = '/path/does/not/exists'
    assert c.local_path is None

def test_new_job():
    def launch_func(a, b=None):
        return a, b
    jname = 'test_job'
    jlocal_path = '/tmp'
    outpath = '/tmp/out'
    c = ClusterClient(local_path=jlocal_path)
    job = c.new_job(launch_func, options={'output': outpath}, jobname=jname)
    assert job.local_path == jlocal_path
    assert job.remote_path == jlocal_path
    assert job.outfile == outpath
    assert job.client == c
    assert c.jobs[0] == job

def test_not_implemented():
    c = ClusterClient()
    with pytest.raises(NotImplementedError):
        c.gen_scripts(None, None)

    with pytest.raises(NotImplementedError):
        c.submit('')
