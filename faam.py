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
import xarray as xr
import dash
import dash_core_components as dcc
import dash_html_components as html

import plotly.offline as py
#py.init_notebook_mode(connected=False)

from netCDF4 import Dataset, num2date
#import cufflinks as cf
#cf.set_config_file(offline=True)

import plotly.io as pio
pio.renderers.default = "browser"

MODE_FULL = 'full'
MODE_1HZ = '1hz'

CORE_REGEX = ('core_faam_(?P<date>[0-9]{8})_v00(?P<version>[0-9])_'
              'r(?P<revision>[0-9]+)_(?P<flightnum>[a-z][0-9]{3})_'
              '?(?P<freq>[1-9]*)h?z?.nc')

def dict_merge(source, destination):
    """ Recursively adds items from source into destination.

    Assumes that keys in source dictionary do not exist in destination.
    If a key already exists in destination it is overwritten with that from
    source. That is, no value merging is done.
    """
    for key, value in source.items():
        if isinstance(value, dict):
            node = destination.setdefault(key, {})
            dict_merge(value, node)
        else:
            destination[key] = value

    return destination


def delay_browser_open(url, sleep=1):
    time.sleep(sleep)
    webbrowser.open(url, new=2)


class FAAMFlight(object):
    def __init__(self, flightnum=None, date=None):
        self.files = {}
        self._version = None
        self._revision = None
        self._freq = None

        self.time = None
        self.flightnum = flightnum
        self.date = date

        self.filters = {
            '=': [],
            '>': [],
            '<': []
        }

    def _vrf_error(self, v=None, r=None, f=None):
        _v = v or self._version
        _r = r if r is not None else self._revision
        _f = f if f is not None else self._freq
        raise ValueError(
            'No data for version {}, revision {}, freq {}'.format(_v, _r, _f)
        )

    def _get_time(self):
        with Dataset(self.file, 'r') as nc:
            self.time = nc['Time'][:].ravel()
            self.time_units = nc['Time'].units
            self.time_calendar = nc['Time'].calendar

    def _reload(self):
        self._get_time()

    def time_at(self, hz=1):

        if self.time is None:
            self._get_time()

        if hz == 1:
            return self.time

        _time = np.empty((len(self.time), hz))
        _time[:, 0] = self.time

        for i in range(1, hz):
            _time[:, i] = _time[:, i-1] + 1 / hz

        return _time.ravel()

    def __getitem__(self, item):
        if type(item) is str:
            items = [item]
        else:
            items = item

        dfs = []

        if self.freq == 0:
            with Dataset(self.file, 'r') as nc:
                items = sorted(items, key=lambda x: nc[x].frequency, reverse=True)
                max_freq = nc[items[0]].frequency

        else:
            max_freq = self.freq

        _df = pd.DataFrame(
            index=self.time_at(hz=max_freq)
        )

        with Dataset(self.file, 'r') as nc:
            for i in items:
                if self.freq == 0:
                    _f = nc[i].frequency
                else:
                    _f = 1

                _d = pd.DataFrame(nc[i][:].ravel(), index=self.time_at(hz=_f))

                _df[i] = _d.reindex(_df.index)


        _index_start = num2date(_df.index[0], units=self.time_units,
                                calendar=self.time_calendar)

        _index_end = num2date(_df.index[-1], units=self.time_units,
                              calendar=self.time_calendar)

        _df.index = pd.date_range(start=_index_start, end=_index_end,
                               periods=len(_df.index))

        return _df


    def plot(self, item):
        self[item].interpolate().iplot()

    def scatter(self, item):
        _df = self[item].dropna()

        _df.iplot(x=item[0], y=item[1:], mode='markers', kind='scatter')

    def slrs(self, min_length=120, roll_lim=3, ps_lim=2, roll_mean=5):
        # TODO: The windowing here assumes 1Hz
        _df = self[['WOW_IND', 'PS_RVSM', 'ROLL_GIN']]

        _df.loc[_df.WOW_IND == 1] = np.nan
        _df.dropna(inplace=True)

        _df['PS_C'] = _df.PS_RVSM.rolling(
            min_length, center=True
        ).std() < ps_lim

        _df['ROLL_C'] = _df.ROLL_GIN.rolling(roll_mean).mean().rolling(
            min_length, center=True
        ).apply(np.ptp, raw=True) < roll_lim

        _df['_SLR'] = (_df['PS_C'] & _df['ROLL_C']).astype(int)
        _df['_SLRCNT'] = (_df._SLR.diff(1) != 0).astype('int').cumsum()
        groups = _df.groupby(_df._SLRCNT)

        slrs = []
        for group in groups:
            _df = group[1]
            if _df._SLR.mean() == 0:
                continue
            if len(_df) < 120:
                continue
            slrs.append(group[1])
        return [i.index for i in slrs]

    def profiles(self, min_length=60, plot=False):
        if self.freq != 1:
            warnings.warn(('WARNING :Profile identification has been tuned to '
                           'work 1 Hz data. YMMV with full rate.'))

        if not self.freq:
            rolling = 60 * 32
            thresh = .15 / 32
            min_length = min_length * 32
        else:
            rolling = 60
            thresh = .15
            min_length = min_length

        _df = self[['PS_RVSM', 'WOW_IND']]
        _df = _df[_df.WOW_IND == 0]
        _df['profile_down'] = 0
        _df['profile_down'].loc[
            _df.PS_RVSM.rolling(rolling, center=True).mean().diff() < thresh
        ] = 1

        _df['_profile_down'] = (_df.profile_down.diff() != 0).astype(int).cumsum()

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

        _df['_profile_up'] = (_df.profile_up.diff() != 0).astype(int).cumsum()

        grp_up = _df.groupby(_df._profile_up)

        profile_up = []
        for group in grp_up:
            _d = group[1]
            if _d.profile_up.mean() != 0:
                continue
            if len(_d) < min_length:
                continue
            profile_up.append(group[1].index)

        if plot:
            plt.plot(_df.PS_RVSM, 'k')

            for _p in profile_up:
                plt.plot(_df.PS_RVSM.loc[_p], linewidth=5,
                                color='r')

            for _p in profile_down:
                plt.plot(_df.PS_RVSM.loc[_p], linewidth=5,
                                color='b')

            plt.show()

        return {
            'ascending': profile_up,
            'descending': profile_down
        }


    def overview(self):
        external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

        app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

        app.layout = html.Div(children=[
            html.H1(children='{} Overview'.format(self.flightnum))
        ])

        t = threading.Thread(
            target=delay_browser_open, args=('http://localhost:8050',)
        )
        t.start()


        app.run_server(debug=False, threaded=True)

    def google_earth(self, outfile=None, launch=True):
        from templates import ge_template
        import subprocess
        df = self[['LAT_GIN', 'LON_GIN', 'ALT_GIN', 'WOW_IND']]

        df = df.asfreq('1S')
        df[df['WOW_IND'] == 1] = np.nan
        df.dropna(inplace=True)

        _coords = [
            '{_lon},{_lat},{_alt}'.format(_lon=_lon, _lat=_lat, _alt=_alt)
            for _lon, _lat, _alt in zip(df.LON_GIN, df.LAT_GIN, df.ALT_GIN)
        ]
        _start = _coords[0]
        _coords_str = '\n'.join(_coords)

        template = ge_template.format(
            fltnum=self.flightnum, date=self.date.strftime('%Y%m%d'),
            start_point=_start, coords=_coords_str
        )

        with open('temp.kml', 'w') as _kml:
            _kml.write(template)
            _kml.write('</Folder></kml>')

        full_path = os.path.join(os.getcwd(), 'temp.kml')

        subprocess.run(['google-earth-pro', full_path])


    @property
    def file(self):
        return self.files[self.version][self.revision][self.freq]

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, version):
        version = int(version)
        try:
            _ = self.files[int(version)][self.revision][self.freq]
            self._version = version
            self._reload()
        except KeyError:
            self._vrf_error(v=version)

    @property
    def revision(self):
        return self._revision

    @revision.setter
    def revision(self, revision):
        revision = int(revision)
        try:
            _ = self.files[self.version][int(revision)][self.freq]
            self._revision = revision
            self._reload()
        except KeyError:
           self._vrf_error(r=revision)

    @property
    def freq(self):
        return self._freq

    @freq.setter
    def freq(self, freq):
        freq = int(freq)
        try:
            _ = self.files[self.version][self.revision][freq]
            self._freq = freq
            self._reload()
        except KeyError:
            self._vrf_error(f=freq)

    def _autoset_file(self):
        self._version = max([i for i in self.files.keys()])
        self._revision = max([i for i in self.files[self._version].keys()])
        self._freq = min(
            [i for i in self.files[self._version][self._revision].keys()]
        )

    def set(self, v=None, r=None, f=None):
        if v is None:
            v = self.version
        if r is None:
            r = self.revision
        if f is None:
            f = self.freq

        try:
            _ = self.files[v][r][f]
            self.version = v
            self.revision = r
            self.freq = f
        except KeyError:
            self._vrf_error(v=v, r=r, f=f)

    def add_file(self, filename):
        rex = re.compile(CORE_REGEX)

        _dirname = os.path.dirname(filename)
        _filename = os.path.basename(filename)

        match = rex.search(_filename)

        _version = int(match['version'])
        _revision = int(match['revision'])
        if match['freq']:
            _freq = int(match['freq'])
        else:
            _freq = 0

        _dict = {
            _version: {
                _revision: {
                    _freq: filename
                }
            }
        }

        self.files = dict_merge(self.files, _dict)
        self._autoset_file()

    def __str__(self):
        return 'FAAMFlight: {}'.format(self.flightnum)

    def __repr__(self):
        return r'FAAMFlight(flightnum={!r}, date={!r})'.format(
            self.flightnum, self.date
        )

    @classmethod
    def from_regex(cls, path, regex):
        _date = datetime.datetime.strptime(regex['date'], '%Y%m%d')
        _fltnum = regex['flightnum']

        _flight = cls(flightnum=_fltnum, date=_date)
        _flight.add_file(os.path.join(path, regex.string))

        return _flight


class FAAMWrapper(object):

    def __init__(self, core_path=None, flightnum=None):
        self.core_path = core_path
        self.flights = {}
        self._populate()

    def __getitem__(self, item):
        return self.flights[item]

    def _populate(self):
        rex = re.compile(CORE_REGEX)

        for root, dirs, files in os.walk(self.core_path):
            for _file in files:
                match = rex.search(_file)
                if match:
                    self._add_file(root, match)


    def _add_file(self, path, regex):
        try:
            self.flights[regex['flightnum']].add_file(
                os.path.join(path, regex.string)
            )
        except KeyError:
            _flight = FAAMFlight.from_regex(path, regex)
            self.flights[regex['flightnum']] = _flight


