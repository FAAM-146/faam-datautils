import numpy as np

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
    hook = 'corefltsum'
    # Override default model in accessor.DataAccessor()
    model = FltSumDataModel
    regex = ('^flight-sum_faam_(?P<date>\d{8})_'
             'r(?P<revision>\d+)_(?P<flightnum>[a-z]\d{3}).(?P<ext>csv|txt)$')


    def _get_profiles(self, items):
        """ Returns start/end times of requested run/s.

        """
        pass


    def _get_runs(self, items):
        """ Returns start/end times of requested run/s.

        """
        pass


    @property
    def runs(self):
        """ Returns start/end times of all runs

        """
        return self._get_runs(items='*')

    @property
    def profiles(self):
        """ Returns start/end times of all profiles

        """
        return self._get_profiles(items='*')


    def _get_event(self, items):
        """ Returns start (and end if applicable) time/s of event item.

        """
        try:
            self.fltsum
        except AttributeError as err:
            if self.ext.lower() == 'csv':
                self._get_csv()
            elif self.ext.lower() == 'txt':
                self._get_txt()

        df = self.fltsum

        if type(items) in [str]:
            items = [items]

        event_mask = np.column_stack([self.fltsum['Event'].str.contains(item,
                                                                        case=False)
                                      for item in items])
        event_rows = df.loc[event_mask.any(axis=1)]


        # blah blah blah