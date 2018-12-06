import os


class Config:
    CARCOSA_PATH_ENV = 'CARCOSA_PATH'
    DEFAULT_PATH = '{home}/.carcosa/'

    def __init__(self):
        pass

    @property
    def path(self):
        path = os.getenv(self.CARCOSA_PATH_ENV)
        if path is None:
            path = self._get_default_path()

        if not os.path.exists(path):
            os.mkdir(path)

        return path

    def _get_default_path(self):
        home = os.getenv('HOME')
        return self.DEFAULT_PATH.format(home=home)


config = Config()
