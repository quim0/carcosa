from typing import Dict, Union, Callable, List
import types
import logging

from .client import ClusterClient
from .cluster import DONE_STATES

from carcosa import scripts


class Job:
    INIT_STATUS = 'carcosa_init'

    def __init__(self,
                 f: Union[Callable, str],
                 s: scripts.Script,
                 o: Dict,
                 client: ClusterClient) -> None:
        """
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
        self.client: ClusterClient = client

        # Status updated when performing an "update"
        self.status: str = INIT_STATUS

        # Metrics, will be filled when the job finishes
        self.metrics: Dict = dict()

        # Parameters needed to construct and launch the scripts
        self.f = f
        self.script = s
        self.options = o

        self.logging.info('Created new job {}'.format(id_))

    @property
    def finished(self):
        if self.state in DONE_STATES:
            return True
        else:
            return False

    def udpate(self) -> None:
        if self.status == INIT_STATUS:
            logging.warning('Job have not been submitted yet. Aborting')
            return

        if self.finished:
            logging.warning('Job already finished')
            return

        self.logging.debug('Updating job {}'.format(self.id)
        server = self.client.server
        id_, status = server.queue_parser(job_id=self.id)

        # This *MUST NOT* happen
        if id_ != self.id:
            self.logging.critical(
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
        if not force and self.status != INIT_STATUS:
            logging.warning('Job have been already lauched. Aborting')
            return
        if not force and self.finished:
            logging.warning('Job have already finished.')
            return
        script_kwargs = dict()
        if isinstance(self.f, types.FunctionType):
            script_kwargs['function'] = self.f
            script_kwargs['args'] = args
            script_kwargs['kwargs'] = kwargs
        elif isinstance(self.f, str):
            script_kwargs['cmd']

        self.client.gen_scripts(
            self.script,
            self.options
            **script_kwargs
            )

        self.id = self.client.submit(self.script)

    def __str__(self):
        return '<JOB-{jid}({status})>'.format(jid=self.id, status=self.status)