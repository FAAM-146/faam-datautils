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

_user_ = 'graeme'
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


def getvars(file, item, grp=None):

    pdb.set_trace()
    if type(item) in [str]:
        items = [os.path.join(grp, item)]
    else:
        items = [os.path.join(grp, i) for i in item]

    # Get list of unique groups. Unfortunately open_dataset must be called
    # for each different group.
    _grps, _vars = zip(*sorted([os.path.split(i_) for i_ in items],
                               key=lambda g: g[0]))

    _grps = [g.replace('/','') for g in _grps[::]]
    _grps_uniq = set(_grps)
    _grps_idx = [[i for i,x in enumerate(_grps)  if x==y] for y in _grps_uniq]

    with Dataset(self.path, 'r') as f:
        pass



faam = FAAM(data_path[_user_]['work'])

flight = 'c224'
grp = ''
items = ['fred/SW_UP_C','/PSAP_FLO','fred/TDEW_CR2']


fred = getvars(faam[flight].core.file,items,grp)

pdb.set_trace()
