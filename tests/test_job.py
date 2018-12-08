from typing import List, Optional, Callable, Dict, Any, Tuple
import pytest

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
        super().__init__(uri=uri,
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


def get_job(opts: List[Any] = []):
    uri = TEST_URI
    local_path = TEST_LOCAL_PATH
    remote_path = TEST_REMOTE_PATH
    c = FakeClient(uri=uri, local_path=local_path, remote_path=remote_path)
    script = scripts.Script(TEST_JOBNAME, local_path, remote_path)
    if len(opts) == 0:
        # Function
        opts.append(lambda x: x*x)
        # Script
        opts.append(script)
        # Options
        opts.append({})
    j = Job(*opts, c)
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
    with pytest.raises(ValueError) as e:
        j.update()

    j.id = TEST_JOB_ID
    j.client.ret_queue_status = TEST_FINISHED_STATUS
    j.update()

    assert j.status == TEST_FINISHED_STATUS
    assert j.finished
    assert not j.running
    assert j.id == TEST_JOB_ID
