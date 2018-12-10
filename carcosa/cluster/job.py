from typing import Dict, Union, Callable, List, Optional, Any
import types
import logging

from .states import DONE_STATES, ACTIVE_STATES

from carcosa import scripts


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

        logging.info('Created new job {}'.format(self.script.name))

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

    @property
    def outfile(self) -> Optional[str]:
        if 'output' in self.options.keys():
            return self.options['output']
        return None

    @property
    def errfile(self) -> Optional[str]:
        if 'error' in self.options.keys():
            return self.options['error']
        return None

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

    def update(self) -> None:
        if not self.launched:
            logging.warning('Job have not been submitted yet. Aborting')
            return

        if self.finished:
            logging.warning('Job already finished')
            return

        logging.debug('Updating job {}'.format(self.id))
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
        """
        if not force and self.status != self.INIT_STATUS:
            logging.warning('Job have been already launched. Aborting')
            return
        if not force and self.finished:
            logging.warning('Job have already finished.')
            return
        script_kwargs: Dict[str, Any] = dict()
        if isinstance(self.f, types.FunctionType):
            script_kwargs['function'] = self.f
            script_kwargs['args'] = args
            script_kwargs['kwargs'] = kwargs
        elif isinstance(self.f, str):
            script_kwargs['cmd'] = self.f

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
