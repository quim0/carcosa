from typing import Tuple, List, Any, Dict, Optional, Iterator, Callable, Union
import types
import Pyro4
import logging
import shutil
import marshal

from carcosa.cluster import ClusterServer, ClusterClient
from carcosa import scripts

SBATCH = 'sbatch'
SQUEUE = 'squeue'
SCANCEL = 'scancel'
SACCT = 'sacct'
OPT_PREFIX = '#SBATCH'


@Pyro4.expose
class SlurmServer(ClusterServer):
    @Pyro4.expose
    @property
    def qsystem(self) -> str:
        return 'slurm'

    def metrics(self,
                job_id: Optional[int] = None) -> Iterator[Tuple[str, ...]]:
        """
        Get job metrics from ``sacct``.
        """
        logging.info('Getting job metrics')

        fields = (
            'JobID', 'Partition', 'AllocCPUs', 'AllocNodes', 'AllocTres',
            'AveCPUFreq', 'AveDiskRead', 'AveDiskWrite', 'AveRSS',
            'ConsumedEnergy', 'Submit', 'Start', 'End', 'Elapsed'
            )
        qargs = [
            'sacct', '-P', '--noheader', '--noconvert',
            '--format={}'.format(','.join(fields))
            ]
        if job_id:
            qargs.append('-j {}'.format(job_id))
        try:
            res = self.run(qargs)
            if res.returncode != 0:
                logging.error('sacct returned non 0.')
                sacct = []
            else:
                out = res.stdout
                sacct = [tuple(i.split('|')) for i in out.split('\n')]
        except Exception:
            logging.error('Error running sacct to get the metrics')
            sacct = []

        for line in sacct:
            yield line

    def queue_test(self) -> bool:
        """
        Check if the slurm queue system is present in the node.

        Returns:
            is_present (bool)
        """
        if shutil.which(SBATCH) is None:
            logging.error('can not find sbatch')
            return False
        if shutil.which(SQUEUE) is None:
            logging.error('can not find squeue')
            return False
        return True

    def submit(self, script_path: str) -> Optional[str]:
        """
        Submit a sbatch job and return its job ID.

        Args:
            script_path (str):
                Path to the sbatch script

        Returns:
            job_id (str):
                ID of the submitted job
        """
        args = [SBATCH, script_path]
        res = self.cmd(args)
        if res.returncode != 0:
            logging.error('sbatch failed with code {}'.format(res.returncode))
            return None

        job_id = res.stdout.split()[-1]
        if '_' in job_id:
            job_id = job_id.split('_')[0]
        return job_id.strip()

    def kill(self, job_ids: List[Union[int, str]]) -> bool:
        """
        Terminate all jobs in job_ids

        Args:
            job_ids (list)

        Returns:
            success (bool)
        """
        job_ids.insert(0, SCANCEL)
        res = self.cmd(job_ids)

        return res.returncode == 0

    def queue_parser(self, job_id: Optional[str] = None) \
            -> Iterator[Tuple[str, ...]]:
        """
        Get the information of the running jobs. Returns the job id and the
        state of the jobs.

        Args:
            job_id (int):
                Job ID to check.
        """
        squeue_fields = ['%A', '%T']
        sacct_fields = ['jobid', 'state']

        squeue_args = [SQUEUE, '-h', '-o {}'.format('|'.join(squeue_fields))]
        sacct_args = [
            SACCT, '-P', '--format={}'.format(','.join(sacct_fields))
            ]
        if job_id is not None:
            sacct_args.append('-j {}'.format(job_id))
            squeue_args.append('-j {}'.format(job_id))

        squeue_out = self.cmd(squeue_args).output
        sacct_out = self.cmd(sacct_args).output

        squeue = list(
            map(lambda x: tuple(x.split('|')), squeue_out.split('\n'))
            )
        sacct = map(lambda x: tuple(x.split('|')), sacct_out.split('\n'))

        # Yield the jobs catched by squeue
        for sinfo in squeue:
            yield sinfo

        # Yield the jobs catched by sacct that are not in squeue
        sjobs = [x[0] for x in squeue]
        for sinfo in sacct:
            jid, _ = sinfo
            if jid not in sjobs:
                yield sinfo


class SlurmClient(ClusterClient):
    def cleanup(self) -> None:
        pass

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
            # A python function will be executed, not a plain command. This
            # implies serializing the function, saving its serialized
            # representation to a file and generating the scripts to load the
            # function again in the remote host.
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

        args = dict(
            precmd=self.parse_options(**options),
            name=script.name,
            command=cmd
            )

        # Generate the sbatch script that will be sent to slurm.
        with open(script.local.sbatch, 'w') as f:
            f.write(
                scripts.SCRIPT_RUNNER.format(args)
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
