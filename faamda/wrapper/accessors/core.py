from .register import register_accessor
from .accessor import DataAccessor

@register_accessor
class CoreAccessor(DataAccessor):
    hook = 'core'
    regex = ('core_faam_(?P<date>[0-9]{8})_v00(?P<version>[0-9])_'
              'r(?P<revision>[0-9]+)_(?P<flightnum>[a-z][0-9]{3})_'
              '?(?P<freq>[1-9]*)h?z?.nc')

@register_accessor
class CoreFltSumAccessor(DataAccessor):
    """Accessor for FAAM core flight summary file

    .. TODO:: Should extension be an input arg to cope with csv/txt versions?
    """
    hook = 'corefltsum'
    regex = ('flight-sum_faam_(?P<date>\d{8})_'
             'r(?P<revision>\d+)_(?P<flightnum>[a-z]\d{3}).csv')
