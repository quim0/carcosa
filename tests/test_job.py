from typing import List, Optional, Callable, Dict, Any, Tuple
import pytest
import tempfile

from carcosa import scripts
from carcosa.cluster import Job, ClusterClient

TEST_JOBNAME: str = 'test_job'
TEST_URI: str = 'test.uri'
TEST_LOCAL_PATH: str = '/tmp/'
TEST_REMOTE_PATH: str = '/tmp/remote'
TEST_JOB_ID: str = '10'
TEST_ACTIVE_STATUS: str = 'running'
TEST_FINISHED_STATUS: str = 'complete'


class FakeClient(ClusterClient):
    def __init__(self,
                 uri: Optional[str] = None,
                 remote_path: Optional[str] = None,
                 local_path: Optional[str] = None) -> None:
        super().__init__(
                uri=uri,
                remote_path=remote_path,
                local_path=local_path
                )
        self.submit_check = False
        self.scripts_check = False
        self.ret_queue_id = TEST_JOB_ID
        self.ret_queue_status = TEST_ACTIVE_STATUS

    @property
    def server(self):
        return self

    def queue_parser(self, job_id: Optional[str] = None) -> Tuple[str, str]:
        r = (self.ret_queue_id, self.ret_queue_status)
        print('>>>>>>>>>>>>>>>>', r)
        return r

    def submit(self, script: scripts.Script) -> str:
        self.submit_check = True
        return TEST_JOB_ID

    def gen_scripts(self,
                    script: scripts.Script,
                    options: Dict,
                    function: Optional[Callable[..., Any]] = None,
                    args: Optional[Tuple] = None,
                    kwargs: Optional[Dict] = None,
                    cmd: Optional[str] = None) -> bool:
        self.scripts_check = True
        return True


def get_job():
    uri = TEST_URI
    local_path = TEST_LOCAL_PATH
    remote_path = TEST_REMOTE_PATH
    c = FakeClient(uri=uri, local_path=local_path, remote_path=remote_path)
    script: scripts.Script = scripts.Script(
        TEST_JOBNAME, local_path, remote_path
        )
    j = Job(lambda x: x*x, script, {}, c)
    return j


def test_init_status():
    j = get_job()
    assert isinstance(j.client, ClusterClient)
    assert j.status == j.INIT_STATUS
    assert not j.launched
    assert not j.finished
    assert not j.running


def test_launch():
    j = get_job()
    j.launch(args=[10])
    # TODO: Improve this test
    assert j.id == TEST_JOB_ID
    assert j.launched
    assert j.client.scripts_check
    assert j.client.submit_check


def test_upate():
    j = get_job()
    j.launched = True
    j.id = TEST_JOB_ID
    j.update()

    assert j.status == TEST_ACTIVE_STATUS
    assert not j.finished
    assert j.running
    assert j.id == TEST_JOB_ID

    j.id = '12345'
    with pytest.raises(ValueError):
        j.update()

    j.id = TEST_JOB_ID
    j.client.ret_queue_status = TEST_FINISHED_STATUS
    j.update()

    assert j.status == TEST_FINISHED_STATUS
    assert j.finished
    assert not j.running
    assert j.id == TEST_JOB_ID


def test_check_local_path():
    j = get_job()
    with pytest.raises(ValueError):
        j.local_path = '/path/does/not/exits/probably'


def test_local_path():
    j = get_job()
    # Don't use windows pls
    j.local_path = '/etc'
    assert j.local_path == '/etc'


def test_remote_path():
    j = get_job()
    j.remote_path = '/etc'
    assert j.remote_path == '/etc'


def test_outfile():
    j = get_job()
    ofile = '/tmp/outfile'
    j.outfile = ofile
    assert j.options['output'] == ofile
    assert j.outfile == ofile


def test_errfile():
    j = get_job()
    efile = '/tmp/errfile'
    j.errfile = efile
    assert j.options['error'] == efile
    assert j.errfile == efile


def test_stdout():
    with tempfile.NamedTemporaryFile(mode='w+') as f:
        test_str = 'testing testing'
        j = get_job()
        j.outfile = f.name
        assert j.stdout is None
        j.launched = True
        assert j.stdout is None
        j.status = 'completed'

        f.write(test_str)
        f.flush()

        assert j.stdout == test_str


def test_stderr():
    with tempfile.NamedTemporaryFile(mode='w+') as f:
        test_str = 'testing testing'
        j = get_job()
        j.errfile = f.name
        assert j.stderr is None
        j.launched = True
        assert j.stderr is None
        j.status = 'completed'

        f.write(test_str)
        f.flush()

        assert j.stderr == test_str
