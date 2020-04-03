import numpy as np
import pandas as pd

from ..models import *
from .register import register_accessor
from .accessor import DataAccessor

__all__ = ['CoreAccessor',
           'CoreFltSumAccessor']


@register_accessor
class CoreAccessor(DataAccessor):
    hook = 'core'
    regex = ('^core_faam_(?P<date>[0-9]{8})_v00(?P<version>[0-9])_'
              'r(?P<revision>[0-9]+)_(?P<flightnum>[a-z][0-9]{3})_'
              '?(?P<freq>[1-9]*)h?z?.nc$')

    def slrs(self, min_length=120, max_length=None, roll_lim=3, ps_lim=2,
             roll_mean=5, freq=1):

        def _add_slrs(slrs, group):
            _gdf = group[1]
            if max_length is None:
                slrs.append(_gdf)
                return
            num_groups = len(_gdf) // max_length
            last_index = -(len(_gdf) % max_length)
            _gdfs = np.split(_gdf.iloc[:last_index], num_groups)
            slrs += _gdfs

            rem = _gdf.iloc[last_index:]
            if len(rem) >= min_length:
                slrs.append(rem)

        window_size = min_length
        slrs = []

        if max_length is not None and max_length < min_length:
            raise ValueError('max_length must be >= min_length')

        _df = self[['WOW_IND', 'PS_RVSM', 'ROLL_GIN']].asfreq('1S')

        # Drop an data on the ground
        _df.loc[_df.WOW_IND == 1] = np.nan
        _df.dropna(inplace=True)

        # Check the variance of the static pressure is sufficiently small
        _df['PS_C'] = _df.PS_RVSM.rolling(
            window_size, center=True
        ).std() < ps_lim

        # Check that the range of the GIN roll is inside acceptable limits
        _df['ROLL_C'] = _df.ROLL_GIN.rolling(roll_mean).mean().rolling(
            window_size, center=True
        ).apply(np.ptp, raw=True) < roll_lim

        # Identify discontiguous regions which pass the selection criteria
        # and group them
        _df['_SLR'] = (_df['PS_C'] & _df['ROLL_C']).astype(int)
        _df['_SLRCNT'] = (_df._SLR.diff(1) != 0).astype('int').cumsum()
        groups = _df.groupby(_df._SLRCNT)

        slrs = []
        for group in groups:
            _df = group[1]
            if _df._SLR.mean() == 0:
                continue
            if len(_df) < window_size:
                continue

            # Add slrs to list, splitting if required
            _add_slrs(slrs, group)

        # Return a list of indicies, at required freq
        return [i.asfreq('{0:0.0f}N'.format(1e9/freq)).index for i in slrs]

    def profiles(self, min_length=60):
        rolling = 60
        thresh = 0.15

        _df = self[['PS_RVSM', 'WOW_IND']].asfreq('1S')
        _df = _df[_df.WOW_IND == 0]
        _df['profile_down'] = 0
        _df['profile_down'].loc[
            _df.PS_RVSM.rolling(rolling, center=True).mean().diff() < thresh
        ] = 1
        _df['_profile_down'] = (
            _df.profile_down.diff() != 0
        ).astype(int).cumsum()

        grp_down = _df.groupby(_df._profile_down)

        profile_down = []
        for group in grp_down:
            _d = group[1]
            if _d.profile_down.mean() != 0:
                continue
            if len(_d) < min_length:
                continue
            profile_down.append(group[1].index)

        _df['profile_up'] = 0
        _df['profile_up'].loc[
            _df.PS_RVSM.rolling(rolling, center=True).mean().diff() > -thresh
        ] = 1
        _df['_profile_up'] = (
            _df.profile_up.diff() != 0
        ).astype(int).cumsum()

        grp_up = _df.groupby(_df._profile_up)

        profile_up = []
        for group in grp_up:
            _d = group[1]
            if _d.profile_up.mean() != 0:
                continue
            if len(_d) < min_length:
                continue
            profile_up.append(group[1].index)

        return {
            'ascending': profile_up,
            'descending': profile_down
        }



@register_accessor
class CoreFltSumAccessor(DataAccessor):
    """Accessor for FAAM core flight summary file

    """
    hook = 'fltsum'
    # Override default model in accessor.DataAccessor()
    model = FltSumDataModel
    fileattrs = ('revision', 'ext')
    regex = ('^flight-sum_faam_(?P<date>\d{8})_'
             'r(?P<revision>\d+)_(?P<flightnum>[a-z]\d{3}).(?P<ext>csv|txt)$')

    def __init__(self, flight):
        self._ext = None
        super().__init__(flight)

    def _autoset_file(self):
        _exts = [i.ext for i in self._files]
        if 'csv' in _exts:
            self.ext = 'csv'
        else:
            self.ext = 'txt'

        super()._autoset_file()

    @property
    def ext(self):
        return self._ext

    @ext.setter
    def ext(self, val):
        self._ext = val

    def _get_manoeuvre(self, kind):
        _fltsum = self.get()
        return [
            i for i in _fltsum
            if kind.lower() in i['event'].lower()
            and i['stop_time'] is not None
        ]

    @property
    def runs(self):
        return self._get_manoeuvre('run')

    @property
    def profiles(self):
        return self._get_manoeuvre('profile')

    @property
    def orbits(self):
        return self._get_manoeuvre('orbit')

    @property
    def nevzorov(self):
        return [i for i in self.runs if 'nevz' in i['comment'].lower()]


    def get_event(self, time, within=None):

        return self.model(self.file)._get_time_event(time, within)


    def get_time(self, event):
        """ Returns (start,stop), or (start,None), times of event.
        """
        return self.model(self.file)._get_event_time(event)


    def index(self, event):
        """ Returns a Datetime slice of event.

        Use core_df.loc[flt_sum.index('event name')]

        Note that DatetimeIndex.indexer_at_time() and
        DatetimeIndex.indexer_between_time() only deal with times, not
        datetimes for some reason.
        """

        pdb.set_trace()

        return slice(*self.get_event(event))


    def events_between(self,times):
        """
        Get mean of times and within = diff(times). However need to cope
        with time strings etc
        """
        pass

        #return self.model(self.file)._get_time_event(time, within)
