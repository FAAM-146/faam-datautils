from .register import register_accessor
from .accessor import DataAccessor

@register_accessor
class CoreCloudAccessor(DataAccessor):
    """Accessor for Core Cloud Physics main nc file

    This file contains (as of Mar 2020) PCASP and CDP data
    """
    hook = 'corecloud'
    regex = ('core-cloud-phy_faam_(?P<date>[0-9]{8})_v(?P<version>[0-9]{3})_'
             'r(?P<revision>[0-9]+)_(?P<flightnum>[a-z][0-9]{3}).nc')


@register_accessor
class CoreCloudCIP15Accessor(DataAccessor):
    """Accessor for Core Cloud Physics CIP15 nc file

    .. TODO:: Should all CIPs have same accessor with hook/regex modified
              by input arg?
    """
    hook = 'corecloudCIP15'
    regex = ('core-cloud-phy_faam_(?P<date>[0-9]{8})_v(?P<version>[0-9]{3})_'
             'r(?P<revision>[0-9]+)_(?P<flightnum>[a-z][0-9]{3})_cip15.nc')


@register_accessor
class CoreCloudCIP25Accessor(DataAccessor):
    """Accessor for Core Cloud Physics CIP25 nc file

    .. TODO:: Should all CIPs have same accessor with hook/regex modified
              by input arg?
    """
    hook = 'corecloudCIP25'
    regex = ('core-cloud-phy_faam_(?P<date>[0-9]{8})_v(?P<version>[0-9]{3})_'
             'r(?P<revision>[0-9]+)_(?P<flightnum>[a-z][0-9]{3})_cip25.nc')


@register_accessor
class CoreCloudCIP100Accessor(DataAccessor):
    """Accessor for Core Cloud Physics CIP100 nc file

    .. TODO:: Should all CIPs have same accessor with hook/regex modified
              by input arg?
    """
    hook = 'corecloudCIP100'
    regex = ('core-cloud-phy_faam_(?P<date>[0-9]{8})_v(?P<version>[0-9]{3})_'
             'r(?P<revision>[0-9]+)_(?P<flightnum>[a-z][0-9]{3})_cip100.nc')


@register_accessor
class CoreCloudCASAccessor(DataAccessor):
    """Accessor for Core Cloud Physics CAS nc file

    """
    hook = 'corecloudCAS'
    regex = ('core-cloud-phy_faam_(?P<date>[0-9]{8})_v(?P<version>[0-9]{3})_'
             'r(?P<revision>[0-9]+)_(?P<flightnum>[a-z][0-9]{3})_cas.nc')
