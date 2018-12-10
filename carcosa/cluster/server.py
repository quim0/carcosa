from typing import Tuple, Union, Optional, Iterator, List
import Pyro4
import subprocess
import os
import errno
import logging

from carcosa import config

from . import errors


class ClusterServer:
    """
    Base class to implement queue management systems.

    .. note::

        The ``qsystem`` property **MUST** me implemented.
    """
    PID_FILE = '{qtype}-{id}.pid'
    URI_FILE = '{qtype}-{id}.uri'
    LOG_FILE = '{qtype}-{id}.log'

    def __init__(self) -> None:
        self._id: Optional[int] = None
        self._daemon: Pyro4.daemon = None

    @property
    def qsystem(self) -> str:
        raise NotImplementedError(
            'A queue systems must be specified by the subclass'
            )

    @property
    def pid_filepath(self) -> str:
        pid_file = self.PID_FILE.format(
            qtype=self.qsystem,
            id=self.id
            )
        return os.path.join(config.path, pid_file)

    @property
    def uri_filepath(self) -> str:
        uri_file = self.URI_FILE.format(
            qtype=self.qsystem,
            id=self.id
            )
        return os.path.join(config.path, uri_file)

    @property
    def log_filepath(self) -> str:
        log_file = self.LOG_FILE.format(
            qtype=self.qsystem,
            id=self.id
            )
        return os.path.join(config.path, log_file)

    @property
    def id(self) -> Optional[int]:
        return self._id

    @classmethod
    def start(cls,
              host: Optional[str] = None,
              port: int = 0) -> Tuple[str, str]:
        """
        Creates a new server instance and create a listening daemon.

        Args:
            qsystem (str):
                Queue type of the client (slurm, torque...)
            host (str, optional):
                Host to bind the server.
            port (int, optional):
                Port to bind the server.

        Returns:
            pid (str):
                PID of the daemon process.
            uri (str):
                URI of the remote server.
        """
        obj = cls()
        pid, uri = obj.daemonize(host=host, port=port)
        return (pid, uri)

    def _get_id(self) -> int:
        """
        Get the current ClusterServer id, this is done by reading the PID files
        in ~/.carcosa to look if there're more server instances running.
        """
        id_ = 0
        for f in os.listdir(config.path):
            f_lst = f.split('.')
            if len(f_lst) != 2:
                continue

            fname, ext = f_lst
            if ext == 'pid':
                fname_lst = fname.split('-')
                if len(fname_lst) == 2 and fname_lst[0] == self.qsystem:
                    id_ += 1

        return id_

    def cmd(self, args) -> subprocess.CompletedProcess:
        # Python 3.5 > required
        logging.info('Executing {}'.format(' '.join(args)))
        return subprocess.run(args)

    def cleanup(self) -> None:
        """
        Remove the pid, uri and log files.
        """
        self._daemon = None
        if os.path.exists(self.pid_filepath):
            os.remove(self.pid_filepath)
        if os.path.exists(self.uri_filepath):
            os.remove(self.uri_filepath)
        if os.path.exists(self.log_filepath):
            os.remove(self.log_filepath)

    def shutdown(self) -> None:
        if self._daemon:
            self._daemon.shutdown()
            self._daemon = None
        else:
            logging.warning(
                'Trying to shutdown a non existing server. Aborting'
                )

    def ping(self) -> str:
        return 'pong'

    def daemonize(self,
                  host: Optional[str] = None,
                  port: int = 0) -> Tuple[str, str]:
        """
        Starts a server in a new process.

        Args:
            host (str):
                Host where the server must be started
            port (int):
                Listening port for the server.

        Returns:
            pid (str):
                PID of the daemon process.
            uri (str):
                URI of the remote server.

        Raises:
            ClusterServerError:
                When the server was not started correctly
        """
        self._id = self._get_id()
        r_fd, w_fd = os.pipe()
        pid = os.fork()
        if pid == 0:
            # Child code

            try:
                # Close the read pipe, as the child will just notify when the
                # process start.
                os.close(r_fd)
                w = os.fdopen(w_fd, 'w')
                with Pyro4.Daemon(host=host, port=port) as daemon:
                    logging.info('Registering daemon')
                    uri = daemon.register(self)
                    self._daemon = daemon

                    with open(self.pid_filepath, 'w') as f:
                        logging.info(
                            'PID {}. Saving to file'.format(os.getpid())
                            )
                        f.write(str(os.getpid()))
                    with open(self.uri_filepath, 'w') as f:
                        logging.info('URI {}. Saving to file'.format(uri))
                        f.write(str(uri))

                    # Avoid IO errors with stderr and stdout, redirect them to
                    # a logfile.

                    # Close stdout and stderr
                    os.close(1)
                    os.close(2)
                    # Logfile will be opened to fd 1 (stdout)
                    with open(self.log_filepath, 'w') as f:
                        # Assign the same file to fd 2 (stderr)
                        os.dup(1)
                        try:
                            logging.info('Starting the Pyro4 server')
                            w.write('ok')
                            daemon.requestLoop()
                        except Exception as e:
                            w.write('ko')
                            logging.error(e)
                            os._exit(1)
            except Exception as e:
                print(e)
            finally:
                # Make sure that always is written something to the pipe, to
                # avoid blocking the parent process.
                w.write('finish')
                w.close()
                os._exit(0)
        else:
            os.close(w_fd)

            # Wait for the child message
            r = os.fdopen(r_fd)
            r.read()
            r.close()

            if not os.path.isfile(self.pid_filepath):
                self.cleanup()
                raise errors.ClusterServerError('PID file not found')

            with open(self.pid_filepath, 'r') as f:
                spid = f.read()

            try:
                os.kill(int(spid), 0)
            except OSError as e:
                # If EPERM is the error we don't have permissions to send the
                # signal to the process, but it exists.
                if e.errno != errno.EPERM:
                    logging.error('Server have not started')
                    self.cleanup()
                    raise errors.ClusterServerError(
                        'Server process does not exist'
                        )

            with open(self.uri_filepath, 'r') as f:
                uri = f.read()

            return (spid, uri)

    # Pure virtual functions
    def metrics(self,
                job_id: Optional[int] = None) -> Iterator[Tuple[str, ...]]:
        """
        ..note::

            Pure virtual, this must be implemented by any subclass.
        """
        raise NotImplementedError('This must be implemented by subclasses.')

    def queue_test(self) -> bool:
        """
        ..note::

            Pure virtual, this must be implemented by any subclass.
        """
        raise NotImplementedError('This must be implemented by subclasses.')

    def submit(self, script_path: str) -> Optional[str]:
        """
        ..note::

            Pure virtual, this must be implemented by any subclass.
        """
        raise NotImplementedError('This must be implemented by subclasses.')

    def kill(self, job_ids: List[Union[int, str]]) -> bool:
        """
        ..note::

            Pure virtual, this must be implemented by any subclass.
        """
        raise NotImplementedError('This must be implemented by subclasses.')

    def queue_parser(self, job_id: Optional[str] = None) \
            -> Iterator[Tuple[str, ...]]:
        """
        ..note::

            Pure virtual, this must be implemented by any subclass.
        """
        raise NotImplementedError('This must be implemented by subclasses.')
