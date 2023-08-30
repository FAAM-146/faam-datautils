import datetime
import re

from netCDF4 import Dataset, num2date

import numpy as np
import pandas as pd

from .abc import DataModel

__all__ = ['CoreNetCDFDataModel']

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

                _data = pd.Series(nc[item][:].ravel().astype(float),
                                  index=self._time_at(self._get_freq(nc[item])))
                df[item] = _data.reindex_like(df, method='bfill', limit=1)

                # _data = nc[item][:].ravel().astype(float)
                # _data[_data.mask] = np.nan
                # _time = self._time_at(self._get_freq(nc[item]))
                # df.loc[_time, item] = _data
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
            # We no want this. We call private method. Dave bad.
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
                # At netCDF4 v1.5.7 this is the default calendar
                self.time_calendar = 'standard'

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
