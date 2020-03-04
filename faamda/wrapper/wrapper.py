import abc
import datetime
import os
import re
import threading
import time
import warnings
import webbrowser

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from netCDF4 import Dataset, num2date

from .accessors import reg_accessors

FULL_FREQ = 101

class FAAMFlight(object):
    def __init__(self, flightnum=None, date=None):
        self.flightnum = flightnum
        self.date = date
        self._accessors = {}

    def __getattr__(self, attr):
        try:
            return self._accessors[attr]
        except KeyError:
            pass

        try:
            return self.__dict__[attr]
        except KeyError:
            pass

        raise AttributeError(
            'Not an an attribute or accessor: {}'.format(attr)
        )

    def add_accessor(self, accessor):
        self._accessors[accessor.hook] = accessor

class FAAMFile(object):
    def __init__(self, path, rex=None):
        self._path = path
        self._rex = rex

        if rex:
            _match = rex.match(os.path.basename(self._path))
            if _match:
                try:
                    self._version = int(_match['version'])
                except (KeyError, ValueError):
                    self._version = None

                try:
                    self._revision = int(_match['revision'])
                except (KeyError, ValueError):
                    self._revision = None

                try:
                    self._freq = int(_match['freq'])
                except (KeyError, ValueError):
                    self._freq = FULL_FREQ

    def __str__(self):
        return self._path

    def __repr__(self):
        return 'FAAMFile({!r}, {!r})'.format(self._path, self._rex)

    @property
    def revision(self):
        return self._revision

    @property
    def version(self):
        return self._version

    @property
    def freq(self):
        if self._freq == FULL_FREQ:
            return 'full'
        return self._freq

class FAAM(object):

    def __init__(self, paths=None):
        self.flights = {}
        if paths is None:
            paths = []
        self._paths = paths

        self._accessors = {}
        for hook, accessor in reg_accessors.items():
            self._accessors[hook] = accessor['regex']

        self._load()

    def __getitem__(self, item):
        return self.flights[item.lower()]

    def _load(self):
        for _path in self._paths:
            for root, dirs, files in os.walk(_path):
                for _file in files:
                    for hook, rex in self._accessors.items():
                        match = rex.search(_file)
                        if match:
                            self.add_file(hook, root, match)

    def add_file(self, hook, root, match):
        flightnum = match['flightnum']
        date = datetime.datetime.strptime(match['date'], '%Y%m%d')

        try:
            flight = self.flights[flightnum]
        except KeyError:
            flight = FAAMFlight(flightnum, date)
            self.flights[flightnum] = flight

        try:
            accessor = getattr(flight, hook)
        except AttributeError:
            accessor = reg_accessors[hook]['class'](flight)
            flight.add_accessor(accessor)

        accessor.add_file(
            FAAMFile(
                os.path.join(root, match.string),
                match.re
            )
        )
