from typing import Dict, Tuple

from . import slurm
from . import local

from carcosa.cluster import ClusterServer, ClusterClient

systems: Dict[str, Tuple[ClusterClient, ClusterServer]] = {
        'slurm': (slurm.SlurmClient, slurm.SlurmServer),
        'local': (local.LocalClient, local.LocalServer)
        }
""" List of strings containing the valid queue systems.
"""

def get_queue_system(qsys: str) -> (ClusterServer, ClusterClient):
    """
    Get the client and the server clases for
    """
    if qsys in systems:
        return systems[qsys]
