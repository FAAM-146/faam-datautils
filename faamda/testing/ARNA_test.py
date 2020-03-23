"""
Script for testing faam-datautils

Uses ARNA-2 processed data files as guinea pig (uses cpps as workhorse)

"""

import os.path
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib as mpl
import matplotlib.pyplot as plt
import glob
import pdb

from netCDF4 import Dataset


_user_ = 'graeme'
_location_ = 'home'


repo_path = {'graeme': os.path.join(os.path.expanduser('~'), 'git'),
             'dave': ''}

import sys
FAAMDA_PATH = os.path.join(repo_path[_user_], 'faam-datautils')
sys.path.insert(0,FAAMDA_PATH)

CPPS_PATH = os.path.join(repo_path[_user_], 'cpps')
sys.path.insert(0,CPPS_PATH)

from faamda.wrapper import FAAM
import cpps

data_path = {'graeme': {'home': ['/home/graeme/Documents/work/ARNA-2/data'],
                        'work': glob.glob('/smb/faam-two/data/Data/cloudphysics/C224*') +
                                glob.glob('/smb/faam-two/data/Data/coreflightdata/data/ncdata/*_c22*')},#*_c22*
             'dave':   {'home': ['/home/daspr/drive/core_processing/2020'],
                        'work': ['/home/daspr/drive/core_processing/2020']}}


#fred = getvars('testing.nc',['time','bob','bin_cal/bin'])
faam = FAAM(data_path[_user_][_location_])

flight = 'c224'
grps = []
core_items = ['fred/SW_UP_C','TDEW_CR2','TDEW_CR2']    # Core nc3
ccp_items = ['CDP_CONC','CDP_FLAG']     # Core Cloud nc3

pdb.set_trace()

fred = faam[flight].core
bob = faam[flight].ccp
alice = faam[flight].ccpCIP15

with faam[flight].core.raw as nc:
  # treat nc as if you'd done Dataset(corefile, 'r') as nc:
  time = nc['Time'][:]


pdb.set_trace()

tc = faam[flight].core._get_time()
tccp = faam[flight].ccp.time()
tccpCIP15 = faam[flight].ccpCIP15.time('pads_raw')


CIP15groups = faam[flight].ccpCIP15._get_groups()


fred = faam[flight].core[core_items]

pdb.set_trace()
