from os import path
from typing import TypeVar, Generic, Optional

SCRIPT_RUNNER = """\
#!/bin/bash
{precmd}
cd {usedir}
date +'%y-%m-%d-%H:%M:%S'
echo "Running {name}"
{command}
exitcode=$?
echo Done
echo Code: $exitcode
date +'%y-%m-%d-%H:%M:%S'
if [[ $exitcode != 0 ]]; then
    echo Exited with code: $exitcode >&2
fi
exit $exitcode
"""

FUNC_RUNNER = """\
import marshal
import types

with open('{marshal_file}', 'rb') as f:
    code, args, kwargs = marshal.load(f)
    function = types.FunctionType(code, globals())
    try:
        out = function(*args, **kwargs)
    except Exception as e:
        out = e

    with open('{out_file}', 'wb') as f:
        marshal.dump(out, f)
"""

T = TypeVar('T')


class Script(Generic[T]):
    """
    Class that manages the script paths.
    """
    def __init__(self,
                 job_name: str,
                 local_path: str,
                 remote_path: str) -> None:
        self.name = job_name
        self.marshal_file = '{}.marshal'.format(job_name)
        self.sbatch_file = '{}.sbatch'.format(job_name)
        self.python_file = '{}.py'.format(job_name)
        self.out_file = '{}.marshal.out'.format(job_name)

        self.local_path = local_path
        self.remote_path = remote_path

        self._mode = 'local'

    @property
    def remote(self) -> 'Script[T]':
        self._mode = 'remote'
        return self

    @property
    def local(self) -> 'Script[T]':
        self._mode = 'local'
        return self

    @property
    def path(self) -> str:
        if self._mode == 'local':
            return self.local_path
        else:
            return self.remote_path

    def filepath(self, f: str) -> Optional[str]:
        try:
            fname = {'marshal': self.marshal_file,
                     'sbatch': self.sbatch_file,
                     'python': self.python_file,
                     'out': self.out_file}[f]
            return path.join(self.path, fname)
        except KeyError:
            return None
