from typing import Callable, Optional
import logging

from . import ClusterClient, ClusterServer
from .errors import ClusterClientError

from carcosa import qsystems


class Cluster():
    def __init__(self,
                 client: ClusterClient,
                 server: ClusterServer,
                 qsystem: str) -> None:
        self._client: ClusterClient = client
        self._server: ClusterServer = server
        if qsystem not in qsystems.systems:
            raise ValueError('Queue system {} is not valid'.format(qsystem))
        self._qsystem: str = qsystem

    @classmethod
    def new(cls,
            local_path: Optional[str] = None,
            remote_path: Optional[str] = None,
            qsystem: str = '',
            uri: Optional[str] = None) -> 'Cluster':
        """
        Create a new cluster insance. This class function asks for parameters
        for initializing the client, if you want to start a server look at
        :meth:`~carcosa.cluster.Cluster.serve`.

        Args:
            local_path:
                Local path for the client.
            remote_path:
                Remote path for the client, if not sepcified the local path is \
                used.
            qsystem:
                Remote queue system, if known (slurm, ...)
            uri:
                Pyro4 uri of the remote server, if known.
        """
        c, s = (None, None)
        if qsystem:
            c, s = qsystems.get_queue_system(qsystem)
        elif uri:
            tmpc = ClusterClient(uri)
            server = tmpc.server
            if not server:
                e_msg = (
                    'Can not connect to the remote server with URI: {uri}'
                    ).format(uri=uri)
                logging.error(e_msg)
                raise ClusterClientError(e_msg)
            qsystem = server.qsystem
            tmpc.disconnect()
            c, s = qsystems.get_queue_system(qsystem)
        else:
            e_msg = (
                'URI or qsystem must be provided to create a Cluster instance'
                )
            logging.error(e_msg)
            raise ValueError(e_msg)

        # Create the instances from the classes
        c = c(uri=uri, local_path=local_path, remote_path=remote_path)
        s = s()
        return cls(c, s, qsystem)

    @classmethod
    def serve(cls,
              host: str = 'localhost',
              port: int = 0,
              qsystem: Optional[str] = None) -> 'Cluster':
        if qsystem:
            c, s = qsystems.get_queue_system(qsystem)
        else:
            pass

        pid, uri = s.start(host=host, port=port)
        c = c(uri=uri)
        return cls(c, s, qsystem)

    @property
    def client(self) -> ClusterClient:
        return self._client

    @property
    def server(self) -> ClusterServer:
        return self._server

    @property
    def qsystem(self) -> str:
        return self._qsystem

    def launch(self, f: Callable) -> bool:
        pass
