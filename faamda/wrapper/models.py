import abc
from netCDF4 import Dataset
import xarray as xr

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


class NetCDFDataModelXR(DataModel):
    """Returns requested data or metadata from path

    This class uses xarray to return a dataArray or dataset with full
    attributes as in the netCDF.

    .. TODO:: Could have option arg to return arrays only?
    """

    def _get_time(self,grp=None):
        """
        If grp in [None,'','/'] then look for time in root group

        """
        try:
            ds = xr.open_dataset(faam[flight].core.file,group=grp)
        except OSError as err:
            # Generally because grp is not a valid file group
            print(err.errno)
            #self.time = None # leave undefined?
        else:
            with ds:
                # Find variable with standard_name 'time'. With CF compliant
                # files this shall cope with different cases of variable name.
                ds_t = ds.filter_by_attrs(standard_name='time')
                time_var = list(ds_t.variables)[0]
                self.time = ds_t[time_var]


    def __getitem__(self, item, grp=None):
        """Reads variable/s from file.

        .. warning::

            Note that it is possible to read variables from different groups
            and put them into a single dataSet. If the dimensions/coordinates
            in those groups are different then horrible things may happen.

        Args:
            var (:obj:`str` or :obj:`list`): Single variable or list of
                variable strings to read. The variable string/s may include
                the full path if groups are involved.
            grp (:obj:`str`): Path to single group, default is None or the
                file root. The same path is prepended to all items if there
                are more than one in addition to any path information in the
                item string/s.
        """

        if type(item) in [str]:
            items = [os.path.join(grp, item)]
        else:
            items = [os.path.join(grp,i_) for i_ in item]



        dfs = []

        with Dataset(self.path, 'r') as f:
            pass
