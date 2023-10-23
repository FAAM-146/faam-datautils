import contextlib

from ..models import *
from .. import wrapper

class DataAccessor(object):
    model = CoreNetCDFDataModel
    fileattrs = ('version', 'revision', 'freq')
    regex = None

    def __init__(self, flight):
        self._files = []
        self._version = None
        self._revision = None
        self._freq = None

        self.flight = flight

    def __getitem__(self, item):
        return self.model(self.file)[item]

    def _autoset_version(self):
        try:
            self._version = max([i.version for i in self._files])
        except (TypeError, ValueError):
            pass
        else:
            self._autoset_revision()

    def _autoset_revision(self):
        try:
            self._revision = max(
                    [i.revision for i in self._filtered_files(revision=None,
                                                              freq=None)]
                    )
        except (TypeError, ValueError):
            pass

    def _autoset_freq(self):
        try:
            self._freq = max([i.freq for i in self.filtered_files])
        except (TypeError, ValueError):
            pass

    def _autoset_file(self):
        self._autoset_version()
        self._autoset_revision()
        self._autoset_freq()

    def _filtered_files(self, **kwargs):
        """Creates list of files in self._files that satisfy condition/s

        Args:
            **kwargs: Any combination of attribute values, with names as are
                given in self.fileattrs. If not given [default] then the
                instance attribute is used, if explicitly given as ``None``
                (or similar) then condition is not used as a filter, or if
                a specific value is used then is used to filter files in
                ``self._files``.

        Returns:
            List of full path and filenames of files. The first filename is
            the one operated on (and returned by ``self.file``).
        """
        _files = self._files

        for attr in self.fileattrs:
            attr_val = kwargs.pop(attr, getattr(self, attr))
            if attr_val in [None, [], '']:
                # Do not filter on this attr
                continue

            # Filter with each loop so are ANDing the attr requirements
            _files = list(filter(lambda x: getattr(x, attr) == attr_val,
                                 _files
                                ))

        return list(_files)


    def get(self, *args, **kwargs):
        """
        Get some sort of data from the DataModel. Implementation is down to the
        DataModel.
        """
        return self.model(self.file).get(*args, **kwargs)

    def find(self, *args, **kwargs):
        """
        Return what's available in the data file, via the DataModel.
        """
        return self.model(self.file).find(*args, **kwargs)

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
    def filtered_files(self):
        """Return list of filename/s that satisfy ``self.fileattrs`` values"""
        return self._filtered_files()

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        """Set version, if current revision is invalid then reset"""
        _version = self._version
        self._version = version
        if not self.filtered_files:
            # Attempt to reset revision number for this version
            self._autoset_revision()
        if not self.filtered_files:
            self._version = _version
            raise ValueError(f"No version v{version} files found.")
        else:
            pass

    @property
    def revision(self):
        return self._revision

    @revision.setter
    def revision(self, revision):
        _revision = self._revision
        self._revision = revision
        if not self.filtered_files:
            self._revision = _revision
            raise ValueError(f"No r{revision} of v{self._version} files found.")

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
        """ Return the file that is currently selected to operate on """
        _files = self.filtered_files
        if len(_files) > 1:
            print('warning: duplicate files')

        return str(_files[0])
