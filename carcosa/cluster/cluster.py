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
            qsystem: str = '',
            uri: Optional[str] = None):
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

        c.uri = uri
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
