import abc
import datetime

from netCDF4 import Dataset, num2date

import numpy as np
import pandas as pd
import xarray as xr

class DataModel(abc.ABC):
    def __init__(self, path):
        self.path = path
        self.time = None

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

class CoreNetCDFDataModel(DataModel):
    def __getitem__(self, item):

        if type(item) is str:
            items = [item]
        else:
            items = item

        with Dataset(self.path, 'r') as nc:
            max_freq = max([nc[i].frequency for i in items])
            df = pd.DataFrame(index=self._time_at(max_freq))

            for item in items:
                _data = nc[item][:].ravel()
                _time = self._time_at(nc[item].frequency)
                df.loc[_time, item] = _data

        return df

    def _time_at(self, freq):
        if self.time is None:
            self._get_time()

        time_start = num2date(self.time[0], units=self.time_units)
        time_end = num2date(
            self.time[-1] + 1,
            units=self.time_units
        )

        index = pd.date_range(
            start=time_start,
            end=time_end,
            freq='{0:0.0f}N'.format(1e9/freq)
        )

        return index[:-1]

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

    def __init__(self):

        self.time = None
        self.groups = []    # should this be ['/']?


    def _get_time(self,grp=None):
        """
        If grp in [None,'','/'] then look for time in root group

        """
        try:
            ds = xr.open_dataset(self.file,group=grp)
        except OSError as err:
            # Generally because grp is not a valid file group
            print(err.errno)
            #self.time = None # or leave undefined?
        else:
            with ds:
                # Find variable with standard_name 'time'. With CF compliant
                # files this shall cope with different cases of variable name.
                ds_t = ds.filter_by_attrs(standard_name='time')
                time_var = list(ds_t.variables)[0]
                self.time = ds_t[time_var]


    def _get_groups(self, grp=None):
        """Determines groups contained within grp of netCDF.
        
        Any additional groups are appended to the list of groups contained in
        self.groups. These strings are the complete paths.

        Args:
            grp (:obj:`str`): Path to single group within the nc file. If grp
                in [None,'','/'] then returns subgroups of root. Default is
                None.
        
        Returns:
            List of subgroup paths. These are the complete path from the root.
            Returns empty list if no groups are found. 
        
        """
        _groups = set(self.groups)

        with Dataset(file, 'r') as ds:        
            if grp in [None,'','/']:
                _groups.update([ds[g].path for g in ds.groups.keys()])
            else:
                try:
                    _groups.update(
                        [ds[grp][g].path for g in ds[grp].groups.keys()])
                except IndexError as err:
                    print(err)

        self.groups = list(_groups)


    def _get_attr(self, attr, grp=None):
        """Returns requested attribute/s from file/group or None if nonexistant.

        """

        try:
            ds = xr.open_dataset(file, group=grp) # self.file?
        except OSError as err:
            # Generally because grp is not a valid file group
            print(err.errno)
            return None

        with ds:
            # discard any attributes that are not in ds
            ds_attr = {k:ds.attrs[k] for k in attr if k in ds.attrs}
            if len(ds_attr) == 0:
                return None

        return ds_attr
 

    def _get_var(self, var, grp=None):
        """Reads requested variable/s from file/group or None if nonexistant.

        """

        try:
            ds = xr.open_dataset(file, group=grp) # self.file?
        except OSError as err:
            # Generally because grp is not a valid file group
            print(err.errno)
            return None

        with ds:
            # discard any variables that are not in ds
            # this also discards any attributes
            ds_var = ds[[v for v in var if v in ds]]
            if len(ds_var.coords) == 0 or len(ds_var.data_vars) == 0:
                return None

        return ds_var

    
    def __getitem__(self, item, grp=None, squeeze=True):
        """Returns item/s from file/group, may be attribute/s or variable/s.

        .. warning::
            Note that requesting both a variable(s) and an attribute(s) does not
            make any sense. If requesting a variable, a dataset is returned. If
            requesting an attribute, the value of that attribute is returned. So
            these two are incompatible. If both variables and attributes are 
            included in item then the attribute request is discarded.

        .. note::
            Variable attributes are returned automatically with the variables.
            Attributes given in `item` are group (including root) attributes.

        Args:
            item (:obj:`str` or :obj:`list`): Single variable or list of
                variable strings to read. The variable string/s may include
                the full path if groups are involved.
            grp (:obj:`str`): Path to single group, default is None or the
                file root. The same path is prepended to all items if there
                are more than one in addition to any path information in the
                item string/s.
            squeeze (:obj:`boolean`): If True [default] then returns single
                dataset with variable/s or None if no variables found. If False
                then returns list, empty or len==1 in these cases. If more than
                one dataset is found then list of datasets is always returned.

        Returns:
            If `squeeze` is False then list of datasets or dictionary of attribute
                key:value pairs. If `squeeze` is True then, if only single
                variable or attribute then returns single dataset or attribute
                value.
        """
        if grp in [None,'','/']:
            grp = ''

        if type(item) in [str]:
            items = [os.path.join(grp, item)]
        else:
            items = [os.path.join(grp, i) for i in item]
        del grp
        
        # Get list of unique groups. Unfortunately open_dataset must be called
        # for each different group.
        _grps, _items = zip(*sorted([os.path.split(i_) for i_ in items],
                                    key=lambda g: g[0]))

        _grps = [g.replace('/','') for g in _grps[::]]
        _grps_uniq = set(_grps)
        _grps_idx = [[i for i,x in enumerate(_grps)  if x==y] for y in _grps_uniq]


        iteml = []
        for idx, grp in zip(_grps_idx, _grps_uniq):
            # Loop through each group and pass all of the items from that group
            iteml.append(self._get_var([_items[i] for i in idx],
                                       grp))

        if len(iteml) == 0:
            # No variables found. Check for attributes

            ### TODO:: This is not a very good way of doing this...
            
            for idx, grp in zip(_grps_idx, _grps_uniq):
                # Loop through each group and pass all of the items from that group
                iteml.append(self._get_attr([_items[i] for i in idx],
                                            grp))

            if squeeze and len(iteml) == 0:
                return None
            if squeeze and len(iteml) == 1:
                # Return only value of single attribute found
                # squeeze should not be used if multiple unknown attribute keys
                # are passed in as a single returned value could belong to any...
                return iteml[iteml.keys()[0]]

        else:
            # Returning variable/s
            if squeeze and len(iteml) == 1:
                # If only single dataset then extract from list and return
                return iteml[0]
        
        return iteml





    @property
    def groups(self, grp=None):
        """ Returns subgroups of grp.
        
        Args:
            grp (:obj:`str`): Path to single group within the nc file. If grp
                in [None,'','/'] then returns subgroups of root. Default is
                None.
        
        Returns:
            List of subgroup paths. These are the complete path from the root.
            Returns empty list if no groups are found. 
        """
        
        # If grp already in self.groups then just extract and return        
        _groups = [g for g in self.groups \
                   if grp.lower() == os.path.dirname(g).lower()]
        if len(_groups) == 0:
            _groups = self._get_groups(grp)
        
        return _groups


    @property
    def allgroups(self):
        """Returns paths to all groups within a file

        """

        # haven't done this yet
        def walktree(top):
            values = top.groups.values()
            yield values
            for value in top.groups.values():
                for children in walktree(value):
                    yield children
