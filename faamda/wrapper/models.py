import abc
import os.path
import re

from netCDF4 import Dataset, num2date

import numpy as np
import pandas as pd
import xarray as xr

import pdb

__all__ = ['CoreNetCDFDataModel',
           'NetCDFDataModel',
           'FltSumDataModel']

IS_ATTRIBUTE = 101
IS_VARIABLE = 102
IS_GROUP = 103
IS_DIMENSION = 104

VARIABLE_STRINGS = ['variable', 'var', 'variables', 'vars']
ATTRIBUTE_STRINGS = ['attribute', 'attr', 'attributes', 'attrs']
GROUP_STRINGS = ['group', 'grp', 'groups', 'grps']
DIMENSION_STRINGS = ['dimension', 'dim', 'dimensions', 'dims']
ROOT_STRINGS = ['', '/']

# Variable attribute names to search when filtering by attribute
SEARCH_ATTRS = ['long_name', 'standard_name', 'comment']

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

        return self._get_if_consistent(items)

    def _get_vars(self, items):
        with Dataset(self.path, 'r') as nc:
            max_freq = max([self._get_freq(nc[i]) for i in items])
            df = pd.DataFrame(index=self._time_at(max_freq))

            for item in items:
                _data = nc[item][:].ravel()
                _data[_data.mask] = np.nan
                _time = self._time_at(self._get_freq(nc[item]))
                df.loc[_time, item] = _data

        return df

    def _get_attrs(self, items):
        _attrs = {}
        with Dataset(self.path, 'r') as nc:
            for item in items:
                _attrs[item] = getattr(nc, item)

        return _attrs

    def _get_if_consistent(self, items):

        def _get_type(nc, item):

            if item in nc.variables:
                return IS_VARIABLE
            if item in nc.ncattrs():
                return IS_ATTRIBUTE

            raise KeyError('{} not found'.format(item))

        _map = {
            IS_VARIABLE: self._get_vars,
            IS_ATTRIBUTE: self._get_attrs
        }

        with Dataset(self.path, 'r') as nc:
            types = [_get_type(nc, item) for item in items]

        if types.count(types[0]) != len(types):
            raise ValueError('Cannot mix variables and attributes')

        return _map[types[0]](items)

    def _get_freq(self, var):
            try:
                return var.shape[1]
            except IndexError:
                return 1

    def _time_at(self, freq):
        if self.time is None:
            self._get_time()

        time_start = num2date(self.time[0], units=self.time_units)
        time_end = num2date(
            self.time[-1] + 1,
            units=self.time_units,
            calendar=self.time_calendar
        )

        try:
            # Newer (?) versions of netCDF4 give datetimes from cftime._cftime
            # We no want this. We call private method. We bad.
            time_start = time_start._to_real_datetime()
            time_end = time_end._to_real_datetime()
        except AttributeError:
            # Not dealing with cftime objects
            pass

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

    def _find_vars(self, filterby):
        _vars = {}
        _filter_attrs = ('long_name', 'standard_name')
        with Dataset(self.path, 'r') as nc:
            if not filterby:
                return {i: nc[i].long_name for i in nc.variables}
            for _var in nc.variables:
                if re.search(filterby, _var, re.IGNORECASE):
                    _vars[_var] = nc[_var].long_name
                    continue
                for _attr in _filter_attrs:
                    if re.search(filterby, getattr(nc[_var], _attr, ''),
                                 re.IGNORECASE):
                        _vars[_var] = nc[_var].long_name
                        continue
        return _vars

    def find(self, what, filterby=None):
        if what.lower() in VARIABLE_STRINGS:
            return self._find_vars(filterby)

        accepted_strs = VARIABLE_STRINGS
        raise ValueError('Can\'t search for {}. Accepted values are {}'.format(
            what, ','.join(accepted_strs)
        ))

    def get(self, items=None, context=None):
        if type(items) is str:
            items = [items]

        if context is None:
            if not items:
                raise ValueError('Neither items or context given')
            return self[items]

        _ret_dict = {}
        with Dataset(self.path, 'r') as nc:
            if context not in nc.variables:
                raise ValueError('Invalid context: {}'.format(context))

            if not items:
                for attr in nc[context].ncattrs():
                    _ret_dict[attr] = getattr(nc[context], attr)
                return _ret_dict

            for item in items:
                _ret_dict[item] = getattr(nc[context], item, None)

        return _ret_dict


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
        """ Returns list of unique groups/paths and associated strs basenames

        xarray.open_dataset must be called individualy on each group in a file
        which is a bit of a pain.

        Args:
            strs (:obj:list of `str`): List of strings to obtain paths from.

        Returns:
            grps_uniq (:obj:`list`): Sorted list of unique group paths. If
                root group then returns path ''.
            grps_strs (:obj:`list`): List of lists of strs' basenames
                associated with each group in grps_unique.
        """

        if type(strs) in [str]:
            strs = [strs[::]]

        # Get list of unique groups.
        _grps, _strs = zip(*sorted([os.path.split(s_) for s_ in strs],
                                    key=lambda g: g[0]))
        _grps = [g.replace('/','') for g in _grps[::]]

        # Create ordered, unique list of group names
        grps_uniq = list(dict.fromkeys(_grps))
        grps_strs = [[_strs[i] for i,x in enumerate(_grps)  if x==y]
                     for y in grps_uniq]

        return grps_uniq, grps_strs


    def _parent_coords(self, items, grp=None):
        """ Finds coordinates of items in parent group/s

        Args:
            items (:obj:`list`): List of variable strings to read. The
                variable strings should have all path information removed
                and all be from the same group, grp.
            grp (:obj:`str`): Path to single group, default is None which
                is the file root. Strings in `ROOT_STRINGS` are not accepted.

        Returns:

        """
        if grp in ROOT_STRINGS:
            grp = None
        try:
            ds = xr.open_dataset(self.path, group=grp)
        except OSError as err:
            # Generally because grp is not a valid file group
            print(err.errno)
            return None

        with ds:
            # Initialise coords dataset with any coordinates that exist
            # Compare with those required for items
            # Coordinate obj do not contain variable attributes. However,
            # since this is initialising the coords, these coordinates are
            # already contained in the dataset and so have all of their attr
            coords_req = ds.coords
            dims_req = ds[items].dims

        while len(coords_req) < len(dims_req):
            if grp in ROOT_STRINGS:
                grp = None
            try:
                ds = xr.open_dataset(self.path, group=grp)
            except OSError as err:
                # Generally because grp is not a valid file group
                print(err.errno)
            else:
                with ds:
                    _coords = ds[[v for v in ds.coords
                                  if (v in dims_req and v not in coords_req)]]
                    coords_req = coords_req.merge(_coords)

            if grp == None:
                break
            grp = os.path.split(grp)[0]

        return coords_req


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


    def _get_attrs(self, items, grp=None, filterby=None, findonly=False):
        """Returns filtered attributes in group.

        This is designed for root/group attributes. If variable attributes are
        required then use `self._get_vars()` as the returned variables
        include their attributes.

        Args:
            items (:obj:`list`): List of attribute name strings to read. The
                attribute strings should have all path information removed
                and all be from the same group, grp. If `items in ['*','all']`
                then all attributes found are returned.
            grp (:obj:`str`): Path to single group, default is None which
                is the file root. Strings in `ROOT_STRINGS` are not accepted.
            filterby (:obj:`str`): String to filter the items by. Attributes
                are filtered by searching for `filterby` in the attribute
                name/s as well as the contents of the attributes.
            findonly (:obj:`bool`): If True then returns only a list of the
                names of any valid attributes that exist.

        Returns:
            Dictionary of all attribute key:value pairs found or a list of
            attribute names if `findonly==True`. If no attributes are
            in dataset then returns None or [] if `findonly==True`.

        """
        with Dataset(self.path, 'r') as _ds:
            if grp == [None]+ROOT_STRINGS:
                ds = _ds
                grp = ''
            else:
                try:
                    ds = _ds[grp]
                except IndexError as err:
                    print(err)
                    return None

            if not set(['*','all','ALL']).isdisjoint(items):
                # If wildcard found in items then return all attributes in grp
                rattr = {os.path.join(grp,a):v for a,v in ds.__dict__.items()}
            else:
                # Return items that are an attribute in group
                rattr = {os.path.join(grp,a):v for a,v in ds.__dict__.items()
                         if a in items}

        if filterby:
            # Search attribute name and contents for filterby string and remove
            # any items in rattr that do not match
            d_keys = [k for k,v in rattr.items()
                      if re.search(filterby,
                                   '{} {}'.format(k,v),
                                   re.IGNORECASE) == None]
            for k in d_keys:
                rattr.pop(k)

        if findonly:
            # If rattr empty then returns []
            return sorted(rattr.keys())

        if len(rattr) == 0:
            return None

        return rattr


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
        # Will this work if v is a dimension but not a coordinate?
        grpvar_func = lambda vlist: \
            [os.path.join(_ds[v].group().name, v) if _ds!=ds else v for v in vlist]

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
                dims_l = [d for d in _ds.dimensions.keys() if filterby.lower() in d.lower()]
            else:
                dims_l = list(_ds.dimensions.keys())

            return grpvar_func(dims_l)

    def _get_dims(self, grp=None, filterby=None):

        pass

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
        grpvar_func = lambda vlist: \
            [os.path.join(_ds[v].path, v) if _ds!=ds else v for v in vlist]

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
                grp_l = [g for g in _ds.groups.keys() if filterby.lower() in g.lower()]
            else:
                grp_l = list(_ds.groups.keys())

        return grpvar_func(grp_l)


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


    def _get_vars(self, items, grp=None, filterby=None, findonly=False):
        """Returns sub-dataset containing filtered data variables in group.

        Args:
            items (:obj:`list`): List of variable strings to read. The
                variable strings should have all path information removed
                and all be from the same group, grp. If `items in ['*','all']`
                then all variables found are returned.
            grp (:obj:`str`): Path to single group, default is None which
                is the file root. Strings in `ROOT_STRINGS` are not accepted.
            filterby (:obj:`str`): String to filter the items by. Variables
                are filtered by searching for `filterby` in the contents of
                variable attributes in SEARCH_ATTRS as well as the variable
                name itself.
            findonly (:obj:`bool`): If True then returns only a dictionary of
                names:description of any valid variables that exist.

        Returns:
            Dataset of all variables found or dictionary of variable name:
            variable description string pairs if `findonly==True`. If
            coordinates are not in grp then they shall be returned as
            dimensions but not coordinates. If no variables in dataset then
            returns None or {} if `findonly==True`.
        """
        try:
            ds = xr.open_dataset(self.path, group=grp)
        except OSError as err:
            # Generally because grp is not a valid file group
            print(err.errno)
            return None

        with ds:
            # If wildcard found in items then make items a list of all vars
            if not set(['*','all','ALL']).isdisjoint(items):
                items = list(ds.data_vars.keys())

            if filterby == None:
                rds = ds[[v for v in items if v in ds]]
            else:
                # Filter variables by long_name, standard_name and variable name
                attr_filter = lambda v: v != None and filterby.lower() in v.lower()

                # .. TODO:: I can't get the below to go at the moment
                # rds_ls = [ds[[v for v in items
                #              if (v in ds and filterby.lower() in v.lower())]]]
                # for attr in SEARCH_ATTRS:
                #     rds_ls.append(ds.filter_by_attrs(eval(attr) = attr_filter))

                rds_ln = ds.filter_by_attrs(long_name = attr_filter)
                rds_sn = ds.filter_by_attrs(standard_name = attr_filter)
                rds_c  = ds.filter_by_attrs(comment = attr_filter)
                rds_vn = ds[[v for v in items
                             if (v in ds and re.search(filterby,
                                                       v,
                                                       re.IGNORECASE)!=None)]]

                # This is not designed to merge different datasets so insist
                # on 'identical' variables if sub-datasets overlap.
                rds = xr.merge([rds_ln, rds_sn, rds_c, rds_vn],
                               compat='identical')

        if findonly:
            # If rds empty then returns {}
            # Search for standard variable description attributes. If none
            # found then add 'no description' dummy string to dict of var names
            search_attr = lambda n: [a if a in rds[n].attrs else 'fred'
                                     for a in SEARCH_ATTRS][0]
            return {os.path.join(grp,n):rds[n].attrs.get(search_attr(n),
                                                         'no description') for n in rds}

        if len(rds.coords) == 0 and len(rds.data_vars) == 0:
            return None

        if len(rds.coords) != len(rds.dims):
            # Coordinates are in a parent group so need to find
            rds_coords = self._parent_coords(list(rds.keys()), grp)
            rds = xr.merge([rds,red_coords])

        return rds


    def find(self, what, grp=None, filterby=None):
        """Finds requested features in file and returns names of those found

        Args:
            what (:obj:`str`): Type of feature to find in self. Must be in
                one of VARIABLE_STRINGS, ATTRIBUTE_STRINGS, GROUP_STRINGS,
                DIMENSION_STRINGS. If requesting feature in a subgroup of
                root then the path can be prepended to the string,
                eg 'data_group/variables' will return variable names in
                /data_group group.

                .. NOTE::
                    what == 'attrs' is not very useful. Probably better to
                    use `get` as will return None if attribute not found anyway.

            grp (:obj:`str`): Path to single group, default is None or the
                file root. This path is prepended to `what` in
                addition to any path information in the `what` string.
            filterby (:obj:`str`): Substring to filter the returned keys by.
                For attributes and groups this shall be a simple regex on the
                name of the attributes/groups. For variables it shall also
                include a `filter_by_attrs()` call to search the `long_name`
                and `standard_name` attributes.

            .. example::
                find('variables','water vapour') returns ['WVSS2F_VMR',
                'WVSS2F_VMR_FLAG','VMR_CR2','VMR_CR2_FLAG','VMR_C_U',
                'VMR_C_U_FLAG'] from the core nc file as 'water vapour' is
                included in an attribute.


        Returns:
            List of variable, attribute, or group names or [] if nothing found.

        """
        if grp in [None]+ROOT_STRINGS:
            _grp, _what = self._uniq_grps(what)
        else:
            _grp, _what = self._uniq_grps(os.path.join(grp,what))

        grp = _grp[0]
        what = _what[0][0]

        if what.lower() in VARIABLE_STRINGS:
            return self._get_vars('*', grp, filterby, findonly=True)

        elif what.lower() in ATTRIBUTE_STRINGS:
            return self._get_attrs('*', grp, filterby, findonly=True)

        elif what.lower() in GROUP_STRINGS:
            raise NotImplementedError
            #return self._find_grps(grp, filterby)

        elif what.lower() in DIMENSION_STRINGS:
            raise NotImplementedError
            #return self._find_dims(grp, filterby)

        else:
            raise NotImplementedError


    def get(self, items, grp=None, filterby=None, fmt=None, squeeze=True):
        """Returns item/s from file/group, may be attribute/s or variable/s.

        .. warning::
            Note that requesting both a variable(s) and an attribute(s) does
            not make any sense. If requesting a variable, a dataset is
            returned. If requesting an attribute, the value of that attribute
            is returned. So these two are incompatible. If both variables
            and attributes are included in `items` then the attribute request
            is discarded.

            This doesn't actually have to happen as each group is in a different
            object in an interable. So there's nothing to stop the return of
            variables from one group and attributes from another. Messy though.

        .. note::
            Variable attributes are returned automatically with the variables.
            Attributes given in `items` are group (including root) attributes.

        Args:
            items (:obj:`str` or :obj:`list`): Single item or list of
                item name strings to read. These string/s may include
                the full path if groups are involved. Items from different
                groups are permissible but probably not all that useful.
                If `items in ['*','all']` then all variables found are
                returned.
            grp (:obj:`str`): Path to single group, default is None or the
                file root. The same path is prepended to all items in
                addition to any path information in the items string/s.
            filterby (:obj:`str`): String to filter the items by. For
                attributes and groups this shall be a simple substring
                search on the name/value of the attributes/groups. Variables
                are filtered by the contents of attributes `long_name` and
                `standard_name` as well as the variable name itself.
            fmt (:boj:`str`): Format of nc file output returned. None [default]
                enables automatic attempt to guess best format.
            squeeze (:obj:`boolean`): If True [default] then returns single
                dataset with variable/s or None if no variables found. If False
                then returns list, empty or len==1 in these cases. If more than
                one dataset is found then list of datasets is always returned.

        Returns:
            If `squeeze` is False then returns;

                    * a dictionary of group:datasets pairs or
                    * a dictionary of attribute name:value pairs.
                    *

                If `squeeze` is True and the len of the above iterables is 1
                then returns a single dataset, attribute, etc. If the len > 1
                then squeeze makes no difference.

        .. code-block:: python

            >>> self._get_attrs(['institution'])
            'FAAM'
            >>> self._get_attrs(['institution','Title'])
            {'institution': 'FAAM', 'Title': 'Data from c224 on 11-Feb-2020'}
            >>> self._get_attrs('institution',squeeze=False)
            {'institution': 'FAAM'}

        """
        # Map item type to appropriate getter
        _map = {IS_VARIABLE: self._get_vars,
                IS_ATTRIBUTE: self._get_attrs,
                IS_GROUP: self._get_groups,
                IS_DIMENSION: self._get_dims}

        if grp in [None]+ROOT_STRINGS:
            grp = ''

        if type(items) in [str]:
            items = [os.path.join(grp, items[::])]
        else:
            items = [os.path.join(grp, i) for i in items[::]]

        pdb.set_trace()
        grps, grp_items = self._uniq_grps(items)

        def _get_type(nc, item):

            if item.lower() in ['*','all']:
                return IS_VARIABLE
            if item in nc.variables:
                return IS_VARIABLE
            if item in nc.ncattrs():
                return IS_ATTRIBUTE
            if item in nc.dimensions:
                return IS_DIMENSION
            else:
                # Work out how to determine if is a group dataset
                pdb.set_trace()

            raise KeyError('{} not found'.format(item))

        # Determine item types for each group, ignore if inconsistent types
        # within a single group
        grp_types = []
        with Dataset(self.path, 'r') as _ds:
            for _grp, _items in zip(grps, grp_items):
                if _grp == '':
                    ds = _ds
                else:
                    try:
                        ds = _ds[_grp]
                    except IndexError as err:
                        print(err)
                        continue

                types = [_get_type(ds, item) for item in _items]
                if types.count(types[0]) != len(types):
                    raise ValueError('Cannot mix variables and attributes')

                grp_types.append(types[0])

        # Loop through each group and return item values
        rd = {}
        for _grp, _items, _type in zip(grps, grp_items, grp_types):

            rd[_grp] = _map[_type](_items, _grp, filterby, False)



        pdb.set_trace()


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




    # def _get_var(self, var, grp=None):
    #     """Reads requested variable/s from file/group or None if nonexistant.

    #     """

    #     try:
    #         ds = xr.open_dataset(self.path, group=grp) # self.file?
    #     except OSError as err:
    #         # Generally because grp is not a valid file group
    #         print(err.errno)
    #         return None

    #     with ds:
    #         # discard any variables that are not in ds
    #         # this also discards any attributes
    #         ds_var = ds[[v for v in var if v in ds]]
    #         if len(ds_var.coords) == 0 or len(ds_var.data_vars) == 0:
    #             return None

    #     return ds_var

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



class FltSumDataModel(DataModel):
    """
    This is the data model for the models.CoreFltSumAccessor()

    """
    pass
