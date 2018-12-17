"""
Classes for local execution of the jobs, without a queue system or a remote
cluster.
There's no queue system so job id is always 0, and queue_parse always returns
'complete'.
LocalServer.complete will not return until the scripts finished its execution.
"""
from typing import Tuple, List, Any, Dict, Optional, Iterator, Callable, Union
import types
import logging

from carcosa.cluster import ClusterServer, ClusterClient
from carcosa import scripts

JOB_ID = '0'
STATUS = 'complete'


class LocalServer(ClusterServer):
    @property
    def qsystem(self) -> str:
        return 'slurm'

    def metrics(self,
                job_id: Optional[int] = None) -> Iterator[Tuple[str, ...]]:
        """
        Get job metrics from ``sacct``.
        """
        logging.info('Getting job metrics')

        yield tuple([''] * 14)

    def queue_test(self) -> bool:
        """
        Check if the slurm queue system is present in the node.

        Returns:
            is_present (bool)
        """
        return True

    def submit(self, script_path: str) -> Optional[str]:
        """
        Execute a job, this will **NOT** return until the script have executed,
        because there's no underlying queue system.

        Args:
            script_path (str):
                Path to the sbatch script

        Returns:
            job_id (str):
                ID of the submitted job
        """
        args = ['bash', script_path]
        res = self.cmd(args)
        if res.returncode != 0:
            logging.error('Local job failed with code {}'.format(
                res.returncode)
                )
            return None
        return JOB_ID

    def kill(self, job_ids: List[Union[int, str]]) -> bool:
        """
        Terminate all jobs in job_ids

        Args:
            job_ids (list)

        Returns:
            success (bool)
        """
        return True

    def queue_parser(self, job_id: Optional[str] = None) \
            -> Iterator[Tuple[str, ...]]:
        """
        Get the information of the running jobs. Returns the job id and the
        state of the jobs.

        Args:
            job_id (int):
                Job ID to check.
        """
        yield (JOB_ID, STATUS)

class LocalClient(ClusterClient):
    @property
    def server(self) -> LocalServer:
        if self._server is None:
            self._server = self._get_server()
        return self._server

    def cleanup(self) -> None:
        pass

    def _get_server(self) -> LocalServer:
        return LocalServer()

    def metrics(self, job_id: int = None) -> Iterator[Tuple[str, ...]]:
        return self.server.metrics(job_id=job_id)

    def gen_scripts(self,
                    script: scripts.Script,
                    options: Dict,
                    function: Optional[Callable[..., Any]] = None,
                    args: Optional[Tuple] = None,
                    kwargs: Optional[Dict] = None,
                    cmd: Optional[str] = None) -> bool:
        """
        Generate the scripts to run the job in slurm. The job to run can be a
        python function or a plaintext command.

        To run a function a ``function``, ``args`` and ``kwargs`` arguments
        must be passed, if a function is passed the ``cmd`` attribute is
        ignored.

        If a function is not passed, a command may be passed through the
        ``cmd`` argument.

        Args:
            local_path (str):
                Path for the job in the local filesystem
            remote_path (str):
                Path for the job in the remote filesystem
            jobname (str):
                Identifier for the job, will be used to generate the scripts
                files
            options (dict):
                Options for sbatch. See :meth:`SlurmClient.parse_options` for
                the available arguments.
            function (types.FunctionType, optional):
                Function to be executed in the queue system
            args (tuple):
                Arguments to be passed to the function (only used if function
                is not None).
            kwargs (dict):
                Keyword arguments passed to the function (only used if function
                is not None)
            cmd (str):
                Command to be executed, if a function is passed this argument
                is not used.

        Returns:
            success (bool)
        """
        if function:
            if not isinstance(function, types.FunctionType):
                raise TypeError(
                    'A function must be passed, not {}'.format(type(function))
                    )

                # Execute the python file that loads and run the target
                # function.
                cmd = 'python {python_file}'.format(
                    python_file=script.remote.filepath('python')
                    )

                # Generate the python file that loads the marshal serialized
                # function, and runs it.
                with open(script.local.filepath('python'), 'w') as f:
                    f.write(
                        scripts.FUNC_RUNNER.format(
                            marshal_file=script.remote.filepath('marshal'),
                            out_file=script.remote.filepath('out')
                            )
                        )

                # Save the serialized function to a file.
                with open(script.local.filepath('marshal'), 'wb') as f:
                    m_obj = (function.__code__, args, kwargs)
                    try:
                        marshal.dump(m_obj, f)
                    except ValueError:
                        logging.error(
                            'Marshal can not serialize some elements.'
                            )
                        self.cleanup()
                        return False

        script_args = dict(
            precmd=self.parse_options(**options),
            name=script.name,
            command=cmd
            )

        # Generate the sbatch script that will be sent to slurm.
        with open(script.local.sbatch, 'w') as f:
            f.write(
                scripts.SCRIPT_RUNNER.format(script_args)
                )

        return True

    def submit(self, script: scripts.Script) -> str:
        """
        Submit a job to slurm and get the job id

        Returns:
            job_id (str)
        """
        return self.server.submit(script.remote.filepath('sbatch'))

    def parse_options(self, **kwargs: Any) -> str:
        """
        Get options and convert it to the apropiate #SBATCH string.

        Args:
            kwargs (dict):
                Available options:

                - ``jname``: string (--job-name)
                - ``time``: string (--time) format: DD-HH:MM:SS
                - ``queue``: string (--qos)
                - ``workdir``: string (--workdir)
                - ``error``: string (--error)
                - ``output``: string (--output)
                - ``nodes``: int (--nodes)
                - ``ntasks``: int (--ntasks)
                - ``cpus_per_task``: int (--cpus-per-task)
                - ``tasks_per_node``: int (--tasks-per-node)
                - ``exclusive``: bool (--exclusive)
        """
        options = []

        strings = {
            'jname': '--job-name',
            'queue': '--qos',
            'workdir': '--workdir',
            'error': '--error',
            'output': '--output'
            }
        for k, v in strings.items():
            if k not in kwargs:
                continue
            options.append(
                '{prefix} {directive}={val}'.format(
                    prefix=OPT_PREFIX,
                    directive=v,
                    val=kwargs[k]
                    )
                )

        ints = {
            'nodes': '--nodes',
            'ntasks': '--ntasks',
            'cpus_per_task': '--cpus-per-task',
            'tasks_per_node': '--tasks-per-node'
            }
        for k, v in ints.items():
            if k not in kwargs:
                continue
            options.append(
                '{prefix} {directive}={val}'.format(
                    prefix=OPT_PREFIX,
                    directive=v,
                    val=str(kwargs[k])
                    )
                )

        bools = {'exclusive': '--exclusive'}
        for k, v in bools.items():
            if k not in kwargs:
                continue
            if kwargs[k]:
                options.append(
                    '{prefix} {directive}'.format(
                        prefix=OPT_PREFIX,
                        directive=v,
                        )
                    )

        return '\n'.join(options)
