from typing import Optional
from random import choice
import string
import types
import Pyro4
import logging

from .job import Job

from carcosa import scripts


class ClusterClient:
    def __init__(self,
                 uri: Optional[str] = None,
                 remote_path: Optional[str] = None,
                 local_path: Optional[str] = None) -> None:
        self._uri = uri
        self._server = None

        self.local_path = local_path
        if remote_path:
            self.remote_path = remote_path
        else:
            self.remote_path = local_path

        # Create a list of executed jobs
        self.jobs: List[Job] = []

    @property
    def uri(self) -> Optional[str]:
        return self._uri

    @uri.setter
    def uri(self, val: str) -> None:
        if not isinstance(val, str):
            raise TypeError('URI must be a string.')
        self._uri = val

    @property
    def server(self) -> Pyro4.Proxy:
        if self._server is None:
            self._server = self._get_server()
        return self._server

    @property
    def connected(self) -> bool:
        return self._server is not None

    @property
    def remote_path(self) -> Optional[str]:
        return self._remote_path

    @remote_path.setter
    def remote_path(self, val: str) -> None:
        logging.warning('Overwriting remote path.')
        self._remote_path = remote_path

    @property
    def local_path(self) -> Optional[str]:
        return self._local_path

    @local_path.setter
    def local_path(self, val: str) -> None:
        if isintance(val, str) and os.path.exists(val):
            logging.warning('Overwriting local path.')
            self._local_path = val
        else:
            logging.error('Local path passed does not exist, aborting.')

    @property
    def local_path(self):
        return self._local_path


    def disconnect(self) -> None:
        if self.server:
            self.server._pyroRelease()
            self._server = None
            logging.info('Disconnected from the server.')
        else:
            logging.warning(
                'Trying to release a non existing server. Aborting.'
                )

        def new_job(self,
                    f: Union[Callable, str],
                    options: Dict = {},
                    jobname: Optional[str] = None) -> Job:
        """
        Get a Job for the function or command passed.

        Args:
            f (function or str):
                It can be a function or a command, to execute in the queue
                system.
            options (dict):
                Options for sbatch, see queue system client.

        Raises:
            ValueError
        """
        if not jobname:
            # XXX: Only works with python 3.6+ ?
            jobname = choice(string.ascii_uppercase + string.digits, k=5)

        if not self.local_path or not self.remote_path:
            logging.error(
                'Local path and remote path must be set before creating a job.'
                )
            raise ValueError('Remote or local path is not set.')

        script = scripts.Script(jobname, self.local_path, self.remote_path)
        if isinstance(f, types.FunctionType):

        elif isinstance(f, str):

        pass

    def _get_server(self, retries: int = 3) -> Optional[Pyro4.Proxy]:
        if not self.uri:
            raise ValueError('Can not connect if URI is not defined.')

        s = Pyro4.Proxy(self.uri)
        for i in range(retries + 1):
            try:
                s._pyroBind()
                break
            except Pyro4.errors.CommunicationError:
                logging.warning(
                    'Can not bind server ({}/{})'.format(i, retries)
                    )

        if i == retries:
            logging.error('Unable to bind to server.')
            return None

        return s