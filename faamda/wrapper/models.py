import abc
import datetime
import os.path

from netCDF4 import Dataset, num2date

import numpy as np
import pandas as pd
import xarray as xr

import pdb

__all__ = ['CoreNetCDFDataModel',
           'NetCDFDataModel',
           'FltSumDataModel']


class DataModel(abc.ABC):
    def __init__(self, path):
        self.path = path
        self.time = None
        self.handle = None

    @abc.abstractmethod
    def __enter__(self):
        """
        Context manager entry
        """

    @abc.abstractmethod
    def __exit__(self, *args):
        """
        Context manager exit
        """

    @abc.abstractmethod
    def __getitem__(self, item):
        """
        Return a variable.
        """

    @abc.abstractmethod
    def get(self):
        """
        Get some data from the models file
        """

    @abc.abstractmethod
    def find(self):
        """
        Return what is available in the model's file, given some hint of what
        to look for
        """

class CoreNetCDFDataModel(DataModel):
    """Returns requested data or metadata from path

    Core netCDF specific model to deal with multi-frequency variables

    """
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

    def __enter__(self):
        self.handle = Dataset(self.path, 'r')
        return self.handle

    def __exit__(self, *args):
        self.handle.close()
        self.handle = None

    def find(self, what):
        raise NotImplementedError

    def get(self, *args, **kwargs):
        raise NotImplementedError


class NetCDFDataModel(DataModel):
    """Returns requested data or metadata from path

    The form of the returned data is selectable by the user and can range
    from a numpy array in the simplest case to a xarray.dataset which
    includes all of the group and variable attributes. Properties that
    return data will make some sort of decision about what type of data
    to return. If;
        attribute then return value
        attributes then return dictionary of key:values
        1-dim variable/s then return pandas dataframe [default]
        n-dim variable/s then return xarray dataset
    """

    # def __init__(self):

    #     self.time = None
    #     self.groups = []    # should this be ['/']?

    def __enter__(self):
        self.handle = Dataset(self.path, 'r')
        return self.handle

    def __exit__(self, *args):
        self.handle.close()
        self.handle = None

    @staticmethod
    def _uniq_grps(strs):
        """ Returns list of unique groups/paths from list of strs

        xarray.open_dataset must be called individualy on each group in a file
        which is a bit of a pain.

        Args:
            strs (:obj:list of `str`): List of strings to obtain paths from.

        Returns:
            grps_uniq (:obj:`list`): List of unique group paths
            grps_idx (:obj:`list`): List of strs indicies associated with each
                group in grps_unique.
        """

        if type(strs) in [str]:
            strs = [strs[::]]

        # Get list of unique groups.
        _grps, _strs = zip(*sorted([os.path.split(s_) for s_ in strs],
                                    key=lambda g: g[0]))

        _grps = [g.replace('/','') for g in _grps[::]]
        grps_uniq = set(_grps)
        grps_idx = [[i for i,x in enumerate(_grps)  if x==y] for y in grps_uniq]

        return list(grps_uniq), grps_idx


    def _find_attrs(self, grp=None, filterby=None):
        """Find attribute names in group grp and filter by filterby

        Args:
            grp (:obj:`str`): Path to single group, default is None or the
                file root.
            filterby (:obj:`str`): Substring to filter the returned keys by.
                For attributes and groups this shall be a simple regex on the
                name of the attributes/groups. For variables it shall also
                include a `filter_by_attrs()` call to search the `long_name` and
                `standard_name` attributes.

        Returns:
            List of attribute names, with full path, or [] if nothing found.

        """
        with Dataset(self.path, 'r') as ds:
            # Should opening file be in calling method?
            if grp in [None,'','/']:
                _ds = ds
            else:
                try:
                    _ds = ds[grp]
                except IndexError as err:
                    print(err)
                    return []

            if filterby:
                return [a for a in _ds.ncattrs() if filterby.lower() in a.lower()]
            else:
                return _ds.ncattrs()


    def _get_attrs(self, attrs, fmt=None, squeeze=True):
        """Returns attributes, simples...if only.

        This is designed for root/group attributes. If variable attributes are
        required then use `self._get_variables()` as the returned variables
        include their attributes.

        Args:
            attrs (:obj:`str` or :obj:`list`): Single attribute or list of
                attribute names to read. The attribute string/s must include
                the full path if groups are involved. If `attrs` in
                ['*','group/all'] then all attributes in the root and in group
                respectively that pass filterby are returned.
            fmt (:boj:`str`): Format of nc file output returned. None [default]
                enables automatic attempt to guess best format.

                Not actually used for attributes?

            squeeze (:obj:`boolean`): If True [default] then returns single
                dataset with variable/s or None if no variables found. If False
                then returns list, empty or len==1 in these cases. If more than
                one dataset is found then list of datasets is always returned.

        Returns:
            Dictionary of attribute names, with full path, and values. Empty
            if no matching attributes found. If squeeze is True and only single
            attribute then just value of attribute returned.

        .. code-block:: python

            >>> self._get_attrs(['institution'])
            'FAAM'
            >>> self._get_attrs(['institution','Title'])
            {'institution': 'FAAM', 'Title': 'Data from c224 on 11-Feb-2020'}
            >>> self._get_attrs('institution',squeeze=False)
            {'institution': 'FAAM'}

        """
        if type(attrs) in [str]:
            _attrs = [attrs]
        else:
            _attrs = attrs

        # Obtain path information from attribute strings
        grps, attr_idx = self._uniq_grps(_attrs)

        with Dataset(self.path, 'r') as ds:
            attr_d = {}
            for attr_l, grp in zip(_attrs[attr_idx], grps):
                if grp in [None,'','/']:
                    _ds = ds
                else:
                    try:
                        _ds = ds[grp]
                    except IndexError as err:
                        print(err)
                        continue

                if set(['*','all','ALL']).isdisjoint(attrs_l):
                    attr_d.update({a:_ds.getncattr(os.path.basename(a)) for a
                                   in attrs_l if a in _ds.ncattrs()})
                else:
                    # Return all attributes in group
                    attr_d.update({a:_ds.getncattr(os.path.basename(a)) for a
                                   in _ds.ncattrs()})

        if squeeze and len(attr_d) == 1:
            return attr_d.value
        else:
            return attr_d


    def _find_dims(self, grp=None, filterby=None):
        """Find dimension names in group grp and filter by filterby

        Args:
            grp (:obj:`str`): Path to single group, default is None or the
                file root.
            filterby (:obj:`str`): Substring to filter the returned keys by.
                For attributes and groups this shall be a simple regex on the
                name of the attributes/groups. For variables it shall also
                include a `filter_by_attrs()` call to search the `long_name` and
                `standard_name` attributes.

        Returns:
            List of dimension names, with full path, or [] if nothing found.

        """

        with Dataset(self.path, 'r') as ds:
            # Should opening file be in calling method?
            if grp in [None,'','/']:
                _ds = ds
            else:
                try:
                    _ds = ds[grp]
                except IndexError as err:
                    print(err)
                    return []

            if filterby:
                return [d for d in _ds.dimensions.keys() if filterby.lower() in d.lower()]
            else:
                return list(_ds.dimensions.keys())


    def _find_grps(self, grp=None, filterby=None):
        """Find group names in group grp and filter by filterby

        Args:
            grp (:obj:`str`): Path to single group, default is None or the
                file root.
            filterby (:obj:`str`): Substring to filter the returned keys by.
                For attributes and groups this shall be a simple regex on the
                name of the attributes/groups. For variables it shall also
                include a `filter_by_attrs()` call to search the `long_name` and
                `standard_name` attributes.

        Returns:
            List of group names, with full path, or [] if nothing found.

        """

        with Dataset(self.path, 'r') as ds:
            # Should opening file be in calling method?
            if grp in [None,'','/']:
                _ds = ds
            else:
                try:
                    _ds = ds[grp]
                except IndexError as err:
                    print(err)
                    return []

            if filterby:
                return [g for g in _ds.groups.keys() if filterby.lower() in g.lower()]
            else:
                return list(_ds.groups.keys())


    def _get_grps(self, grps, fmt='xr', squeeze=True):
        """Returns dictionary of datasets associated with each group in grps.

        Args:
            grps (:obj:`str` or :obj:`list`): Single group name or list of
                groups to interogate. Root is indicated by either '' or '/'.
            fmt (:boj:`str`): Format of nc file output returned. Should be
                one of 'pd' for a pandas dataframe or 'xr' [default] for an
                xarray dataset.
            squeeze (:obj:`boolean`): If True [default] reduces dimensionality
                when possible.

        Returns:
                If squeeze is True then returns single dataset with variable/s
                or None if no groups or group variables found. If False
                then returns dictionary, empty or len==1 in these cases. If more
                than one group is found then list of datasets is always
                returned. Dictionary keys are the groups in grp.

        """

        ### Not written yet

        if type(attrs) in [str]:
            _grps = [grps]
        else:
            _grps = grps

        # Obtain path information from attribute strings
        grps, attr_idx = self._uniq_grps(_attrs)

        with Dataset(self.path, 'r') as ds:
            attr_d = {}
            for attr_l, grp in zip(_attrs[attr_idx], grps):
                if grp in [None,'','/']:
                    _ds = ds
                else:
                    try:
                        _ds = ds[grp]
                    except IndexError as err:
                        print(err)
                        continue

            attr_d.update({a:_ds.getncattr(os.path.basename(a)) for a
                           in attrs_l if a in _ds.ncattrs()})

        if squeeze and len(attr_d) == 1:
            return attr_d.value
        else:
            return attr_d


    def _find_vars(self, grp=None, filterby=None):
        """Find variable names in group grp and filter by filterby

        Args:
            grp (:obj:`str`): Path to single group, default is None or the
                file root.
            filterby (:obj:`str`): Substring to filter the returned keys by.
                For attributes and groups this shall be a simple regex on the
                name of the attributes/groups. For variables it shall also
                include a `filter_by_attrs()` call to search the `long_name` and
                `standard_name` attributes.

        Returns:
            List of variables, with full path, or [] if nothing found.

        """
        pdb.set_trace()

        # Concat groups and variable name
        grpvar_func = lambda vlist: [os.path.join(_ds[v].group().name, v) for v in vlist]

        with Dataset(self.path, 'r') as ds:
            # Should opening file be in calling method?
            if grp in [None,'','/']:
                _ds = ds
            else:
                try:
                    _ds = ds[grp]
                except IndexError as err:
                    print(err)
                    return []

            if filterby == None:
                return grpvar_func(_ds.variables)

            # Search for variables and filter by long_name, standard_name, and variable name
            attr_filter = lambda v: v != None and filterby.lower() in v.lower()

            vars_ln = [v.name for v in _ds.get_variables_by_attributes(long_name = attr_filter)]
            try:
                vars_ln = grpvar_func(vars_ln[::])
            except KeyError as err:
                # group is 'empty' as is the root so no name attribute
                # is there a better catch for this?
                pass

            vars_sn = [v.name for v in _ds.get_variables_by_attributes(standard_name = attr_filter)]
            try:
                vars_sn = grpvar_func(vars_sn[::])
            except KeyError as err:
                pass

            vars_vn = [v for v in _ds.variables if filterby.lower() in v.lower()]
            try:
                vars_vn = grpvar_func(vars_vn[::])
            except KeyError as err:
                pass

            return list(set(vars_ln).union(vars_sn, vars_vn))



    def find(self, what, filterby=None):
        """Finds requested features in file and returns those found

        Args:
            what (:obj:`str`): Type of feature to find in self. Must be in
                ['variables', 'vars', 'attributes', 'attrs', 'groups', 'grps'].
                Note that if requesting feature in a subgroup of root then the
                path should be prepended to the string,
                eg 'data_group/variables' will return variable names in
                /data_group group.

                what == 'attrs' probably is not very useful. Probably better to
                use `get` as will return None if attribute not found.

            filterby (:obj:`str`): Substring to filter the returned keys by.
                For attributes and groups this shall be a simple regex on the
                name of the attributes/groups. For variables it shall also
                include a `filter_by_attrs()` call to search the `long_name` and
                `standard_name` attributes.

            .. example::
                find('variables','water vapour') returns ['WVSS2F_VMR',
                'WVSS2F_VMR_FLAG','VMR_CR2','VMR_CR2_FLAG','VMR_C_U',
                'VMR_C_U_FLAG'] from the core nc file as 'water vapour' is
                included in an attribute.


        Returns:
            List of variable, attribute, or group names or [] if nothing found.

        """
        # Obtain any path information from `what` arg
        grp, _ = self._uniq_grps(what)

        if os.path.basename(what).lower() in ['variables', 'vars']:
            return self._find_vars(grp[0], filterby)

        elif os.path.basename(what).lower() in ['attributes', 'attrs']:
            return self._find_attrs(grp[0], filterby)

        elif os.path.basename(what).lower() in ['groups', 'grps']:
            return self._find_grps(grp[0], filterby)

        elif os.path.basename(what).lower() in ['dimensions', 'dims']:
            return self._find_dims(grp[0], filterby)

        else:
            raise NotImplementedError


    def _get(self, item, grp=None, fmt=None, squeeze=True):
        """
        Returns item/s from file/group, may be attribute/s or variable/s.

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
            fmt (:boj:`str`): Format of nc file output returned. None [default]
                enables automatic attempt to guess best format.



            Need to work squeeze

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

        guess_fmt = {'str': {'in': [lambda i: type(i) in [str]],
                             'out': lambda o: str(o)},
                     'df':  {'in': [lambda i: type(i) in []],
                             'out': lambda o: o}
                    }

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


    def _get_ds(self,grp=None):
        """Sets entire dataset for group.

        Note that this uses load_dataset so that the nc file is closed
        immediately.

        """
        if grp in [None,'','/']:
            grp = None

        try:
            self.ds = xr.load_dataset(self.path,group=grp)
        except OSError as err:
            # Generally because grp is not a valid file group
            print(err.errno)
            #self.time = None # or leave undefined?


    def _get_df(self,grp=None):
        """Sets entire dataframe for group.

        Just converts a xr.dataset into a pd.dataframe. Some file attributes
        shall be lost in the translation.

        """
        self.df = self._get_ds(grp).to_dataframe()



    def _get_time(self,grp=None):
        """Sets self.time property based on time coordinate of dataset group

        Args:
            grp (:obj:`str`): Path to single group within the nc file. If grp
                in [None,'','/'] then returns time coordinate of root otherwise
                returns time coord of grp. Default is None.

        """

        if grp in [None,'','/']:
            grp = None

        try:
            ds = xr.open_dataset(self.path,group=grp)
        except OSError as err:
            # Generally because grp is not a valid file group
            print(err.errno)
            #self.time = None # or leave undefined?
        else:
            with ds:
                # Will only return time/Time if it is a coordinate variable
                time_var = [v for v in ds.coords if 'time' in v.lower()]

                # What to do if there is more than one? Is this possible?
                self.time = ds[time_var[0]]


    def time(self,grp=None):

        if self.time == None:
            self._get_time(grp=grp)

        return self.time


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




    def _get_var(self, var, grp=None):
        """Reads requested variable/s from file/group or None if nonexistant.

        """

        try:
            ds = xr.open_dataset(self.path, group=grp) # self.file?
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

    def get(self, *args, **kwargs):
        raise NotImplementedError

    def __getitem__(self, item):


        """
        This uses _find_x() to filter and obtain full paths then passes this to
        _get_x() to return results

        """


        return 'fred'

    ### No workie!! __getitem__ cannot accept args

    """
    So __getitem__ will return data in a default structure based on what was
    asked for. Write a series of getters that allow the user to include
    arguments to request the desired output format.

    """


### No workie!! Properties cannot accept args

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


    @property
    def dims(self):
        return self._dims




class FltSumDataModel(DataModel):
    """
    This is the data model for the models.CoreFltSumAccessor()

    """
    pass
