from typing import Dict, Union, Callable, List, Optional, Any, TYPE_CHECKING
import marshal
import types
import logging
import os

from .states import DONE_STATES, ACTIVE_STATES

from carcosa import scripts

if TYPE_CHECKING:
    # This is a cyclic dependency at runtime, but it's necessary when
    # performing the type checking.
    from carcosa.cluster import ClusterClient


class JobResultError(Exception):
    """
    Raised when the marshal file of the result value is invalid.
    """
    pass


class Job:
    INIT_STATUS: str = 'carcosa_not_launched'

    def __init__(self,
                 f: Union[Callable, str],
                 s: scripts.Script,
                 o: Dict,
                 client: 'ClusterClient') -> None:
        """
        TODO: Cluster client type annotation
        Args:
            f (types.Function or str):
                Function or command to execute.
            s (scripts.Script):
                Script object with the paths to generate the scripts.
            o (dict):
                Options for the batch system scripts. It can be an empty dict.
            client (ClusterClient):
                Client object.
        """
        self.id: Optional[str] = None
        self.client: 'ClusterClient' = client

        self.launched: bool = False

        # Status updated when performing an "update"
        self.status: str = self.INIT_STATUS

        # Metrics, will be filled when the job finishes
        # TODO
        self.metrics: Dict = dict()

        # Parameters needed to construct and launch the scripts
        self.f = f
        self.script = s
        self.options = o

        # Make sure that stdout and stderr is saved
        if not self.outfile:
            self.outfile = self.script.name + '.out'

        if not self.errfile:
            self.errfile = self.script.name + '.err'

        logging.info('Created new job {}'.format(self.script.name))

    # Path related properties

    @property
    def local_path(self) -> Optional[str]:
        return self.script.local_path

    @local_path.setter
    def local_path(self, val: str) -> None:
        if isinstance(val, str) and os.path.exists(val):
            logging.warning('Overwriting local path: new path {}'.format(val))
            self.script.local_path = val
        else:
            logging.error('Local path passed does not exist.')
            raise ValueError('Invalid local path, path does not exist.')

    @property
    def remote_path(self) -> Optional[str]:
        return self.script.remote_path

    @remote_path.setter
    def remote_path(self, val: str) -> None:
        logging.warning('Overwriting remote path: new path {}'.format(val))
        self.script.remote_path = val

    @property
    def outfile(self) -> Optional[str]:
        if 'output' in self.options.keys():
            return self.options['output']
        return None

    @outfile.setter
    def outfile(self, val: str) -> None:
        self.options['output'] = val

    @property
    def errfile(self) -> Optional[str]:
        if 'error' in self.options.keys():
            return self.options['error']
        return None

    @errfile.setter
    def errfile(self, val: str) -> None:
        self.options['error'] = val

    # Status properties

    @property
    def finished(self) -> bool:
        if self.status.lower() in DONE_STATES:
            return True
        return False

    @property
    def running(self) -> bool:
        if self.status.lower() in ACTIVE_STATES:
            return True
        return False

    # Data properties

    @property
    def stdout(self) -> Optional[str]:
        if not self.launched:
            logging.warning('Job have not been submitted yet. Aborting')
            return None

        if not self.finished:
            logging.warning('Job have not finished yet')
            return None

        if self.outfile:
            with open(self.outfile, 'r') as f:
                return f.read()
        return None

    @property
    def stderr(self) -> Optional[str]:
        if not self.launched:
            logging.warning('Job have not been submitted yet. Aborting')
            return None

        if not self.finished:
            logging.warning('Job have not finished yet')
            return None

        if self.errfile:
            with open(self.errfile, 'r') as f:
                return f.read()
        return None

    @property
    def retval(self) -> Any:
        """
        Gets the return value for the job
        """
        if not self.finished:
            logging.warning(
                'Job have not finished yet, won\'t read result file.'
                )
            return

        if not os.path.isfile(self.script.out_file):
            logging.error(
                'Result marshal file does not exist! Aborting. ({})'.format(
                    self.script.out_file
                    )
                )
            raise FileNotFoundError('Marshal file not found')

        with open(self.script.out_file, 'rb') as f:
            try:
                v = marshal.load(f)
                if isinstance(v, Exception):
                    raise v
                elif isinstance(v, type) and issubclass(v, Exception):
                    raise v
                else:
                    return v
            except (EOFError, ValueError, TypeError) as e:
                logging.error(
                    'Error loading the result marshal file: {}'.format(e)
                    )
                raise JobResultError()

    # Methods

    def update(self) -> None:
        if not self.launched:
            logging.warning('Job have not been submitted yet. Aborting')
            return

        if self.finished:
            logging.warning('Job already finished')
            return

        server = self.client.server
        id_, status = server.queue_parser(job_id=self.id)

        # This *MUST NOT* happen
        if id_ != self.id:
            logging.critical(
                'Queue system id for the job is not the same as the local job '
                'id. This *MUST NOT* happen and probably there\'s a bug in '
                'the code.'
                )
            raise ValueError('Local job id is different from remote job id')

        self.status = status

        logging.info(
            'Job {} updated, new status: {}'.format(self, self.status)
            )

    def launch(self,
               args: List = [],
               kwargs: Dict = {},
               force: bool = False) -> None:
        """
        Launch a job to the queue system.

        Args:
            args (list):
                If the job is a python function, the arguments for it.
            kwargs (dict):
                If the job is a python function, the keyword arguments for it.
            force (bool):
                Relaunch the job even if it have been already launched.

        Raises:
            ValueError:
                If local or remote paths are not set, as it won't be able to
                generate the scripts.
        """
        if not force and self.status != self.INIT_STATUS:
            logging.warning('Job have been already launched. Aborting')
            return
        if not force and self.finished:
            logging.warning('Job have already finished.')
            return

        if not self.local_path or not self.remote_path:
            e_msg = (
                'Local and remote paths must be set before launching a job.'
                )
            logging.error(e_msg)
            raise ValueError(e_msg)

        script_kwargs: Dict[str, Any] = dict()
        if isinstance(self.f, types.FunctionType):
            script_kwargs['function'] = self.f
            script_kwargs['args'] = args
            script_kwargs['kwargs'] = kwargs
        elif isinstance(self.f, str):
            script_kwargs['cmd'] = self.f

        logging.info('Launching job with options: {}'.format(self.options))

        self.client.gen_scripts(
            self.script,
            self.options,
            **script_kwargs
            )

        self.id = self.client.submit(self.script)
        self.launched = True

        logging.info('Job launched with id {}'.format(self.id))

    def __str__(self):
        return '<JOB-{jid}({status})>'.format(jid=self.id, status=self.status)
