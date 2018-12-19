from typing import Optional, List, Union, Callable, Dict, Iterator, Tuple, Any
from random import choices
from time import sleep
import string
import types
import os
import Pyro4
import logging

from .job import Job

from carcosa import scripts


class ClusterClient:
    def __init__(self,
                 uri: Optional[str] = None,
                 remote_path: Optional[str] = None,
                 local_path: Optional[str] = None) -> None:
        """
        Args:
            uri (str, optional):
                Pyro4 URI where the server is running. This is optional in the
                contructor because can be set later, but it **must** be set
                before connection (first access to :py:attr:`~.server`).
            local_path (str, optional):
                Path where the jobs will create the scripts and write the
                output files in local host. Usually it's mounted from
                remote_path. This is optional, if it's not set, it **must** be
                set later for each job individually.
            remote_path (str, optional):
                Path where the jobs will create the scripts and write the
                output files in the remote server. This is optional, if it's
                not set, it **must** be set later for each job individually.
                If remote_path is not set, but local_path is, it'll assume that
                they're the same.
        """
        self._uri = uri
        self._server = None

        self.local_path = local_path
        self.remote_path = remote_path
        if not remote_path and local_path:
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
        # TODO: Ping server
        return self._server is not None

    @property
    def remote_path(self) -> Optional[str]:
        """
        If a remote path is set, this will be used a the default for all the
        jobs. If not, a remote path **must** be set individually for each new
        job.
        """
        return self._remote_path

    @remote_path.setter
    def remote_path(self, val: str) -> None:
        logging.warning('Overwriting remote path.')
        self._remote_path = val

    @property
    def local_path(self) -> Optional[str]:
        """
        If a local path is set, this will be used a the default for all the
        jobs. If not, a local path **must** be set individually for each new
        job.
        """
        return self._local_path

    @local_path.setter
    def local_path(self, val: str) -> None:
        if val is None:
            self._local_path = None
        elif isinstance(val, str) and os.path.isdir(val):
            logging.warning('Overwriting local path.')
            self._local_path = val
        else:
            e_msg = 'Local path passed does not exist.'
            logging.error(e_msg)
            raise ValueError(e_msg)

    def disconnect(self) -> None:
        if self.server:
            self.server._pyroRelease()
            self._server = None
            logging.info('Disconnected from the server.')
        else:
            logging.warning(
                'Trying to release a non existing server. Aborting.'
                )

    def new_job(self, f: Union[Callable, str],
                options: Dict = {},
                jobname: Optional[str] = None) -> Job:
        """
        Get a Job for the function or command passed.

        Available options are (for slurm):
        - ``output`` (str): File where the stdout of the launched job will be saved.
        - ``error`` (str): File where the stderr will be saved.
        - ``jname`` (str): Job for the name (if different from the name in the
           :class:`~carcosa.scripts.Script` object).
        - ``time`` (str): Walltime, in format ``DD-HH:MM:SS``.
        - ``queue`` (str): Queue to launch the job (``--qos``).
        - ``workdir`` (str): Workdir of the job, if different from remote_path.
        - ``nodes`` (int): Number of nodes to use.
        - ``ntasks`` (int): Numbers of tasks to spawn.
        - ``cpus_per_task`` (int): Self explainatory.
        - ``tasks_per_node`` (int): Self explainatory.
        - ``exclusive`` (bool): Use the nodes in exclusive mode.

        Args:
            f (Union[Callable, str]):
                It can be a function or a command, to execute in the queue
                system.
            options (dict, optional):
                Options for sbatch, see queue system client.
            jobname (str, optional):
                Job of the name

        Returns:
            job (Job):
                New job created.
        """
        if not jobname:
            # XXX: Only works with python 3.6+ ?
            jobname = ''.join(
                choices(string.ascii_uppercase + string.digits, k=6)
                )

        if (not isinstance(f, types.FunctionType) and
                not isinstance(f, str)):
            raise TypeError(
                'Job work must be a python function or a cmd string'
                )

        if not self.local_path or not self.remote_path:
            logging.warning(
                'Local or remote path not set in client, if it\'s not set for '
                'this job, carcosa won\'t be able to run it.'
                )

        script = scripts.Script(jobname, self.local_path, self.remote_path)
        j = Job(f, script, options, self)

        self.jobs.append(j)

        return j

    def launch_job(job: Optional[Job] = None,
                   args: List = [],
                   kwargs: Dict = {},
                   retries: int = 3) -> Job:
        """
        Launches a job, with correct error handling. If not job is provided, it
        gets the las job that was created in the client.

        Args:
            job (:class:`Job`, optional):
                Job to be launched, if no job is passed, it'll use the last one
                created at this client.
            args (list, optional):
                Arguments for the job, if the job will execute a python
                function.
            kwargs (dict, optional):
                Keyword arguments for the job, if the job will execute a python
                function.
            retries (int, optional):
                Number of times that the Job will be resubmitted in case of
                error.

        Raises:
            ValueError:
                Can't get the job object because there was no provided, and the
                client job list is empty.
                Local or remote path are not set for the job, and can not be
                obtained from the client.
            ConnectionError:
                The connection with the remote server
        """
        if not job:
            if len(self.jobs) > 0:
                job = self.jobs[-1]
            else:
                raise ValueError(
                    'No job was provided, and client jobs list is empty.'
                    )

        try:
            job.launch(args, kwargs)
            return job
        except ValueError as e:
            logging.warning(
                'Local or remote paths for the job are not set, trying to '
                "get them from the client."
                )

            # Try to fill the job local and remote path with the client data, if
            # it exists
            local_path_set = False
            remote_path_set = False

            if not job.local_path:
                if self.local_path:
                    logging.info('Local path set from client data')
                    job.local_path = self.local_path
                    local_path_set = True
            else:
                local_path_set = True

            if not job.remote_path:
                if self.remote_path:
                    logging.info('Remote path set from client data.')
                    job.remote_path = self.remote_path
                    remote_path_set = True
            else:
                remote_path_set = True

            if not (local_path_set and remote_path_set):
                logging.error('Can not set local or remote path for the job')
                raise e
            else:
                # Relaunch the job if possible
                self.launch_job(job, args, kwargs, retries)

        except Pyro4.errors.ConnectionClosedError:
            logging.error(
                'Connection was closed while trying to launch the job'
                )
            # TODO: call disconnect ?
            # TODO: async time wait ?
            # Wait some time (arbitrary), reset the server and try to reconnect
            time.sleep(1)
            self._server = None
            if self.retries > 0:
                logging.warning(
                    'Could not start the job due to a connection error, '
                    'retying ({})'.format(retries))
                return self.launch_job(job, args, kwargs, retries - 1)
            logging.critical(
                'Could not launch the job'
                )
            raise ConnectionError(
                'Con not connect to the remote server to launch the job'
                )

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

    def metrics(self, job_id: int = None) -> Iterator[Tuple[str, ...]]:
        return self.server.metrics(job_id=job_id)

    # Pure virtual functions (to be implemented by the queue system subclasses)

    def gen_scripts(self,
                    script: scripts.Script,
                    options: Dict,
                    function: Optional[Callable[..., Any]] = None,
                    args: Optional[Tuple] = None,
                    kwargs: Optional[Dict] = None,
                    cmd: Optional[str] = None) -> bool:
        raise NotImplementedError('This must be implemented in any subclass')

    def submit(self, script: scripts.Script) -> str:
        raise NotImplementedError('This must be implemented in any subclass')
