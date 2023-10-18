import datetime
import os.path
import re

from netCDF4 import Dataset

import xarray as xr

from .abc import DataModel

__all__ = ['NetCDFDataModel']

IS_ATTRIBUTE = 101
IS_VARIABLE = 102
IS_DIMENSION = 103
IS_GROUP = 104

ATTRIBUTE_STRINGS = ['attribute', 'attr', 'attributes', 'attrs']
VARIABLE_STRINGS = ['variable', 'var', 'variables', 'vars']
DIMENSION_STRINGS = ['dimension', 'dim', 'dimensions', 'dims']
GROUP_STRINGS = ['group', 'grp', 'groups', 'grps']
ROOT_STRINGS = ['', '/']

# Variable attribute names to search when filtering by attribute
SEARCH_ATTRS = ['long_name', 'standard_name', 'comment']


class NetCDFDataModel(DataModel):
    """Returns requested data or metadata from path

    Generalised netCDF model which will handle netCDF4 files with groups.
    Default format is xr.Dataset as these will contain all group and
    variable attributes.
    """

    def __enter__(self):
        self.handle = Dataset(self.path, 'r')
        return self.handle

    def __exit__(self, *args):
        self.handle.close()
        self.handle = None

    def __getitem__(self, item):
        return self.get(item, squeeze=True)


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

        .. NOTE::
            It would probably be faster to have this as a @staticmethod and
            pass it the netCDF4 dataset. Then not re-opening file. However
            are netCDF4 coords compatable with xr.Coordinates?

        Args:
            items (:obj:`list`): List of variable strings to read. The
                variable strings should have all path information removed
                and all be from the same group, grp.
            grp (:obj:`str`): Path to single group, default is None which
                is the file root. Strings in `ROOT_STRINGS` are not accepted.

        Returns:
            Dataset of coordinates. Should be merged in calling method. There
            probably should be some catch for if the wrong coords are found?
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

        import pdb
        pdb.set_trace()

        while len(coords_req) < len(dims_req):
            # Step up one level in path
            try:
                grp = os.path.split(grp)[0]
            except TypeError as err:
                # Because grp is None
                break

            if grp in ROOT_STRINGS:
                grp = None
            try:
                ds = xr.open_dataset(self.path, group=grp)
            except OSError as err:
                # Generally because grp is not a valid file group
                print(err.errno)
            else:
                with ds:
                    # Add coordinate that is the same name and length as that
                    # required and is not already in coords_req
                    _coords = ds[[v for v in ds.coords
                                  if (v in dims_req and
                                      len(ds[v]) == dims_req[v] and
                                      v not in coords_req)]]
                    coords_req = coords_req.merge(_coords)

        return coords_req


    def _find_attrs(self, items, grp=None, filterby=None):
        """ Returns list of attribute names.

        """
        d = self._get_attrs(items, grp, filterby)
        if d is None:
            return None

        # If d empty then returns []
        return sorted(d.keys())


    def _get_attrs(self, items, grp=None, filterby=None):
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

        Returns:
            Dictionary of all attribute key:value pairs found or {}.

        Raises:
            IndexError if group grp not found in dataset.

        """
        with Dataset(self.path, 'r') as _ds:
            if grp in [None]+ROOT_STRINGS:
                ds = _ds
                grp = ''
            else:
                try:
                    ds = _ds[grp]
                except IndexError as err:
                    # Group grp not in _ds
                    raise

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
        pass
        # # Will this work if v is a dimension but not a coordinate?
        # grpvar_func = lambda vlist: \
        #     [os.path.join(_ds[v].group().name, v) if _ds!=ds else v for v in vlist]

        # with Dataset(self.path, 'r') as ds:
        #     # Should opening file be in calling method?
        #     if grp in [None,'','/']:
        #         _ds = ds
        #     else:
        #         try:
        #             _ds = ds[grp]
        #         except IndexError as err:
        #             print(err)
        #             return []

        #     if filterby:
        #         dims_l = [d for d in _ds.dimensions.keys() if filterby.lower() in d.lower()]
        #     else:
        #         dims_l = list(_ds.dimensions.keys())

        #     return grpvar_func(dims_l)


    def _get_dims(self, grp=None, filterby=None):

        pass


    def _find_grps(self, grp=None, filterby=None):
        """Returns paths to all groups within a file.

        http://unidata.github.io/netcdf4-python/netCDF4/index.html#section2

        Args are ignored.

        Returns:
            List of group paths starting with the root, '/'.
        """
        def walktree(top):
            values = top.groups.values()
            yield values
            for value in top.groups.values():
                for children in walktree(value):
                    yield children

        grps_list = ['/']
        with Dataset(self.path, 'r') as nc:
            for children in walktree(nc):
                for child in children:
                    grps_list.append(child.path)

        return sorted(grps_list)


    def _get_grps(self, items, grp=None, filterby=None):
        """Returns dictionary of datasets associated with each group in grp.

        Args:
            items (:obj:`list`): List of group strings. The strings should
                have all path information removed and all be from the same
                group, grp. If `items in ['*','all']` then all groups found
                are returned.
            grp (:obj:`str`): Path to single group, default is None which
                is the file root. Strings in `ROOT_STRINGS` are not accepted.
            filterby (:obj:`str`): String to filter the items by. Groups
                are filtered by searching for `filterby` in the group name/s.

        Returns:
            Dictionary of group:dataset pairs, one for each group found or
            an empty dictionary if no groups found.

        Raises:
            IndexError if group grp not found in dataset.
        """
        with Dataset(self.path, 'r') as _ds:
            if grp in [None]+ROOT_STRINGS:
                ds = _ds
                grp = ''
            else:
                try:
                    ds = _ds[grp]
                except IndexError as err:
                    # Group grp not in _ds
                    raise

            if not set(['*','all','ALL']).isdisjoint(items):
                items = list(ds.groups.keys())

            if filterby:
                _grps = [ds[g].path for g in items
                         if (g in ds.groups and re.search(filterby,
                                                         g,
                                                         re.IGNORECASE)!=None)]
            else:
                _grps = [ds[g].path for g in items if g in ds.groups]

        rd = {}
        for _grp in _grps:
            _rds = xr.load_dataset(self.path, group=os.path.basename(_grp))
            _rds_coords = self._parent_coords(list(_rds.keys()), _grp)
            rd[_grp] = xr.merge([_rds,_rds_coords])

        return rd


    def _find_vars(self, items, grp=None, filterby=None):
        """ Returns dictionary of variable name:variable description pairs.

        """
        ds = self._get_vars(items, grp, filterby)
        if ds is None:
            return {}

        # If ds empty then returns {}.
        # Search for standard variable description attributes. If none
        # found then add 'no description' dummy string to dict of var names
        search_attr = lambda n: [a if a in ds[n].attrs else 'fred'
                                 for a in SEARCH_ATTRS][0]
        v = {os.path.join(grp,n):ds[n].attrs.get(search_attr(n),
                                                 'no description') for n in ds}

        return v


    def _get_vars(self, items, grp=None, filterby=None):
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

        Returns:
            Dataset of all variables found or an empty dataset if no
            variables in dataset.

        Raises:
            OSError if attempt to open netCDF file with a nonexistant group.

        .. TODO::
            Should netCDF4 and xr group errors be made consistent? Currently
            IndexError from netCDF4 and OSError from xarray.
        """
        try:
            ds = xr.open_dataset(self.path, group=grp)
        except OSError as err:
            # Generally because grp is not a valid file group
            raise

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

        if len(rds.coords) == 0 and len(rds.data_vars) == 0:
            return xr.Dataset()

        if set(rds.coords) != set(rds.dims):
            # Some coordinates are in a parent group so need to find
            rds_coords = self._parent_coords(list(rds), grp)
            try:
                rds = xr.merge([rds,rds_coords])
            except TypeError as err:
                # Sometimes the rds_coords is a DatasetCoordinates object
                # these cannot be merged with a Dataset.
                # This seems to occur with 'wrong' coordinates in v6xx core
                # cloud netCDFs
                # No error checking is done with .update so errors may occur
                # warnings.warn('xr.merge() failed so falling back to .update(). '
                #               'No internal error checking is done.')
                rds.update(rds_coords)

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
                    `what == 'attrs'` is not very useful. Probably better to
                    use `get()`.

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
                included in the variable attributes.

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
            return self._find_vars('*', grp, filterby)

        elif what.lower() in ATTRIBUTE_STRINGS:
            return self._find_attrs('*', grp, filterby)

        elif what.lower() in GROUP_STRINGS:
            return self._find_grps()

        elif what.lower() in DIMENSION_STRINGS:
            raise NotImplementedError
            #return self._find_dims(grp, filterby)

        else:
            raise ValueError('Unknown nc feature requested. Should be one '
                             "of 'attrs', 'vars', 'dims', 'groups'.")


    def get(self, items, grp=None, filterby=None, fmt=None, squeeze=True):
        """Returns item/s from file/group, may be attribute/s or variable/s.

        .. warning::
            Note that requesting both a variable(s) and an attribute(s) does
            not make any sense. If requesting a variable, a dataset is
            returned. If requesting an attribute, the value of that attribute
            is returned. So these two are incompatible. If both variables
            and attributes are included in `items` a ValueError is raised.

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
            fmt (:obj:`str`): Format of nc file output returned. None [default]
                or 'xr' then returns xarray Dataset. If 'pd' then returns
                pandas dataframe. If 'np' returns dictionary of numpy arrays
                of all variables.
            squeeze (:obj:`boolean`): If True [default] then returns single
                dataset with variable/s or None if no variables found. If False
                then returns list, empty or len==1 in these cases. If more than
                one dataset is found then list of datasets is always returned.

        Returns:
            If `squeeze` is False then returns a dictionary is returned with
                the key being the group path. For each group key the item
                depends on what is being requested and may be;

                    * for attributes, a dictionary of attribute name:value
                    pairs.
                    * for variables, a dictionary of group:datasets pairs
                    * for groups, a dictionary of group: dataset pairs, the
                    dataset of which is the entire group.

                If `squeeze` is True and the len of the above iterables is 1
                then returns a single dataset, attribute, etc. If the len > 1
                then squeeze makes no difference.

        Raises:
            IndexError: If group grp is not found in netCDF file.
            ValueError: If both variables and attributes are requested from
                the same group.

        .. code-block:: python

            >>> self.get(['institution'])
            'FAAM'
            >>> self.get(['institution','Title'])
            {'institution': 'FAAM', 'Title': 'Data from c224 on 11-Feb-2020'}
            >>> self._get_attrs('institution',squeeze=False)
            {'institution': 'FAAM'}

        """
        # Map item type to appropriate getter
        _map = {IS_VARIABLE: self._get_vars,
                IS_ATTRIBUTE: self._get_attrs,
                IS_GROUP: self._get_grps,
                IS_DIMENSION: self._get_dims}

        if grp in [None]+ROOT_STRINGS:
            grp = ''

        if type(items) in [str]:
            items = [os.path.join(grp, items[::])]
        else:
            items = [os.path.join(grp, i) for i in items[::]]

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
            if item in nc.groups:
                return IS_GROUP

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
                        # grp does not exist in _ds. Probably want a better
                        # way of dealing with multi-group items.
                        raise

                types = [_get_type(ds, item) for item in _items]
                if types.count(types[0]) != len(types):
                    # Mixed types in single group. Probably want a better
                    # way of dealing with multi-group items.
                    raise ValueError('Cannot mix variables and attributes')

                grp_types.append(types[0])

        # Loop through each group and return item values
        rd = {}
        for _grp, _items, _type in zip(grps, grp_items, grp_types):
            rd[os.path.join('/',_grp)] = _map[_type](_items, _grp, filterby)

            if fmt == None or fmt.lower() in ['xr','xarray']:
                pass
            elif fmt.lower() in ['pd','pandas']:
                # Some error checking required?
                rd[_grp] = rd[_grp].to_dataframe()
            elif fmt.lower() in ['np','numpy']:
                raise NotImplementedError

        if squeeze and len(rd) == 1:
            return rd[list(rd.keys())[0]]

        return rd


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


    @property
    def ncgroups(self):

        return self._find_grps()
