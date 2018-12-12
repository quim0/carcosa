import pytest
import os

from carcosa.cluster import ClusterServer
from carcosa import config

TEST_QSYSTEM = 'test'
TEST_ID = '1'

class TServer(ClusterServer):
    @property
    def qsystem(self) -> str:
        return TEST_QSYSTEM


def test_qsystem():
    s = ClusterServer()
    with pytest.raises(NotImplementedError):
        s.qsystem


def test_pid_file():
    s = TServer()
    with pytest.raises(ValueError):
        s.pid_filepath

    s._id = TEST_ID

    path = os.path.join(
        config.path,
        s.PID_FILE.format(qtype=TEST_QSYSTEM, id=TEST_ID)
        )

    assert s.pid_filepath == path

def test_uri_file():
    s = TServer()
    with pytest.raises(ValueError):
        s.uri_filepath

    s._id = TEST_ID

    path = os.path.join(
        config.path,
        s.URI_FILE.format(qtype=TEST_QSYSTEM, id=TEST_ID)
        )

    assert s.uri_filepath == path


def test_log_file():
    s = TServer()
    with pytest.raises(ValueError):
        s.log_filepath

    s._id = TEST_ID

    path = os.path.join(
        config.path,
        s.LOG_FILE.format(qtype=TEST_QSYSTEM, id=TEST_ID)
        )

    assert s.log_filepath == path


def test_get_id():
    s = TServer()
    s._id = s._get_id()
    assert s.id == 0

    with open(s.pid_filepath, 'w+') as f:
        f.write('1')

    assert s._get_id() == 1

    os.remove(s.pid_filepath)

    assert s._get_id() == 0
