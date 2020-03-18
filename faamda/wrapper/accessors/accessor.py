import contextlib

from ..models import *
from .. import wrapper

class DataAccessor(object):
    model = CoreNetCDFDataModel
    regex = None

    def __init__(self, flight):
        self._files = []
        self._version = None
        self._revision = None
        self._freq = None

        self.flight = flight

    def __getitem__(self, item):
        return self.model(self.file)[item]

    def _get_time(self):
        self.model(self.file)._get_time()

    def _autoset_file(self):
        self._version = max([i.version for i in self._filtered_files])
        self._revision = max([i.revision for i in self._filtered_files])
        self._freq = max([i.freq for i in self._filtered_files])

    @property
    @contextlib.contextmanager
    def raw(self):
        """
        Defines a context manager to access the file handler in the underlying
        DataModel.

        """
        with self.model(self.file) as h:
            yield h

    @property
    def _filtered_files(self):
        _files = self._files
        for attr in ('version', 'revision', 'freq'):

            if getattr(self, attr) is not None:
                _files = list(filter(
                    lambda x: getattr(x, attr) == getattr(self, attr),
                    _files
                ))

        return list(_files)

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        _version = self._version
        self._version = version
        if not self._filtered_files:
            self._version = _version
            raise Exception('cannae do it')

    @property
    def revision(self):
        return self._revision

    @revision.setter
    def revision(self, revision):
        self._revision = revision

    @property
    def freq(self):
        if self._freq == wrapper.FULL_FREQ:
            return 'full'
        return self._freq

    @freq.setter
    def freq(self, freq):
        if freq is None:
            self._freq = wrapper.FULL_FREQ
            return
        self._freq = freq

    def add_file(self, ffile):
        self._files.append(ffile)
        self._autoset_file()

    @property
    def file(self):
        _files = self._files

        for attr in ('version', 'revision', 'freq'):
            if getattr(self, attr) is not None:
                _files = list(filter(
                    lambda x: getattr(x, attr) == getattr(self, attr),
                    _files
                ))

        if len(_files) > 1:
            print('warning: duplicate files')

        return str(_files[0])
