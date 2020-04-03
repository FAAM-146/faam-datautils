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

#CPPS_PATH = os.path.join(repo_path[_user_], 'cpps')
#sys.path.insert(0,CPPS_PATH)

from faamda.wrapper import FAAM
#import cpps

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

fred = faam[flight].core
bob = faam[flight].ccp
alice = faam[flight].ccpCIP15
eve = faam[flight].fltsum

pdb.set_trace()

# Sort out proper test data files and turn into unit tests?

# Test .__getitem__ for flt summary
run_1a = eve['Run 1']
run_1b = eve[pd.datetime(2020, 2, 11, 15, 10, 28)]
run_1c = eve['20200211T15:10:28']

# Test accessor methods for flt summary
event_t = eve.get_event('20200211T1538','60 sec')
time_e = eve.get_time('Run 1')

try:
    eve_df = eve.to_df()   # no workie
except:
    pass

fltsum = eve.get()


# Test non-existant group
try:
    fred_find1 = fred.find('vars','fred')
except IndexError as err:
    print(err)

grps_find = alice.find('groups')

# Test attribute find
bob_find1 = bob.find('attrs')

# Test sub-group variables find
alice_find1 = alice.find('PADS_group/vars',filterby='diameter')

# Test root variable get
alice_get1 = alice.get(['altitude'])

# Test group get + parent coordinate, unsqueezed
alice_get2 = alice.get('PADS_group',squeeze=False)

# Test group get, squeezed
alice_get3 = alice.get('PADS_group',squeeze=True)

# Test filtering
alice_get4 = alice.get('PADS_group/*',filterby='flag')  # Why is None?

# Test attribute single group
alice_get5 = alice.get(['institution','title'])

# Test mixed type in single group
try:
    alice_get6 = alice.get(['institution','altitude'])
except ValueError as err:
    print(err)

# Test mixed types in multiple groups
try:
    alice_get7 = alice.get(['institution','PADS_group/cip15_lwc_pads'])
except ValueError as err:
    print(err)

# Test variables in multiple groups
alice_get8 = alice.get(['altitude','PADS_group/cip15_lwc_pads'])

pdb.set_trace()

with faam[flight].core.raw as nc:
  # treat nc as if you'd done Dataset(corefile, 'r') as nc:
  time = nc['Time'][:]


