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


@register_accessor
class CoreFltSumAccessor(DataAccessor):
    """Accessor for FAAM core flight summary file

    """
    hook = 'corefltsum'
    # Override default model in accessor.DataAccessor()
    model = FltSumDataModel
    regex = ('^flight-sum_faam_(?P<date>\d{8})_'
             'r(?P<revision>\d+)_(?P<flightnum>[a-z]\d{3}).csv$')
