import abc
from netCDF4 import Dataset

class DataModel(abc.ABC):
    def __init__(self, path):
        self.path = path

    @abc.abstractmethod
    def __getitem__(self, item):
        """
        Return a variable.
        """

    @abc.abstractmethod
    def _get_time(self):
        """
        Return the dataset time array.
        """

class NetCDFDataModel(DataModel):
    def __getitem__(self, item):
        raise RuntimeError('Need to do some work here!')

        if type(item) is str:
            items = [item]
        else:
            items = item

        dfs = []

        with Dataset(self.path, 'r') as f:
            pass


    def _get_time(self):
        with Dataset(self.path, 'r') as nc:
            self.time = nc['Time'][:].ravel()
            self.time_units = nc['Time'].units
            try:
                self.time_calendar = nc['Time'].calendar
            except AttributeError:
                self.time_calendar = None
