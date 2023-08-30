import csv
import re
import datetime

import pandas as pd

from .abc import DataModel

__all__ = ['FltSumDataModel']


class FltSumDataModel(DataModel):
    """
    This is the data model for the models.CoreFltSumAccessor()

    The flight summary data model produces a list of ordered dictionaries, one
    for each event in the flight summary. The structure of each dictionary is;

    ..codeblock::
        OrderedDict(
          [('event', descriptive text of event, eg 'Run 2'),
           ('start_time', datetime.datetime of start of event),
           ('start_hdg', float of degrees heading at start of event),
           ('start_height', float of pressure height [kft] at start of event),
           ('start_lat', float of latitude [N] at start of event),
           ('start_lon', float of longitude [E] at start of event),
           ('stop_time', datetime.datetime of end of event or None),
           ('stop_hdg', float of degrees heading at end of event or None),
           ('stop_height', float of pressure height [kft] at end of event or None),
           ('stop_lat', float of latitude [N] at end of event or None),
           ('stop_lon', float of longitude [E] at end of event or None),
           ('comment': string comment)])


    """
    def __enter__(self):
        self.handle = open(self.path, 'r')
        return self.handle

    def __exit__(self, *args):
        self.handle.close()
        self.handle = None

    def __getitem__(self, item):
        """ Returns an event based on the event name or the time.
        """
        try:
            return self._get_time_event(item, within=None)
        except TypeError as err:
            # Not a recognised timestamp
            pass

        for event in self.get():
            if event['event'].lower() == item.lower():
                return event
        raise KeyError('Event, {}, not found.'.format(item))


    def to_df(self):
        """ Creates dataframe from list of dictionaries
        """
        return pd.DataFrame.from_dict(
                    {d_['start_time']:d_ for d_ in self.get()},
                                      orient='index')#.set_index('start_time',
                                                     #           inplace=True)


    def _get(self):
        return getattr(self, '_get_{}'.format(self.path.split('.')[-1]))


    def _get_csv(self):
        """
        Returns .csv flight summary
        """

        _fieldnames = [
            'event', 'start_time', 'start_hdg', 'start_height', 'start_lat',
            'start_lon', 'stop_time', 'stop_hdg', 'stop_height', 'stop_lat',
            'stop_lon', 'comment'
        ]

        ret_list = []
        with open(self.path, 'r') as _csv:
            reader = csv.DictReader(_csv, fieldnames=_fieldnames)
            for row in reader:
                ret_list.append(row)

        for item in ret_list[1:]:
            for var in ('start_time', 'stop_time'):
                try:
                    item[var] = datetime.datetime.strptime(
                                            item[var], '%Y-%m-%d %H:%M:%S')
                except KeyError as err:
                    item[var] = None
                    continue
                except ValueError as err:
                    try:
                        item[var] = datetime.datetime.strptime(
                                            item[var], '%Y-%m-%d %H:%M')
                    except ValueError as err:
                        try:
                            item[var] = datetime.datetime.strptime(
                                            item[var], '%Y-%m-%d %H')
                        except Exception as err:
                            item[var] = None
                            continue

            for var_t in ('start', 'stop'):
                for var_o in ('hdg', 'height', 'lat', 'lon'):
                    var = '{}_{}'.format(var_t, var_o)
                    if not item[var]:
                        item[var] = None
                        continue
                    item[var] = float(item[var])

        return sorted(ret_list[1:], key=lambda x: x['start_time'])


    def _get_txt(self, metarows=9, fltdate=None, **kwargs):
        """  Reads .txt flight summary and puts contents into self.fltsum.

        Note that the text flight summaries were originally somewhat free-form.
        The metadata was not filled out consistently. The main issue that this
        causes is that the date may not be able to be parsed correctly, eg
        the year is missing.

        Args:
            metarows (:obj: `int`): Number of header lines to skip before the
                first row of data. Default is 9.
            fltdate (:obj: `date` or `str`): The date is required to combine
                with the start and stop timestamps to create a datetime obj.
                This can be obtained from the metadata however it can be
                specified in ``fltdate`` if required. The default is ``None``
                which forces the metadata date to be used, if this cannot be
                parsed a ``ValueError`` will be raised.
            **kwargs: User can change or add columns to be read from the
                text file using kwargs. The key is the column name and the
                value is a tuple of fixed width interval. Note that changing
                and left hand column width will probably change column
                intervals to the right.

        ..TODO::
            ``_fieldnames`` is a dictionary of column names and intervals.
            The order of the names is the order in which they are read. There
            is currently no means for the user to modify the order.

        """

        # Define column heading names and associated fixed column widths
        _fieldnames = {'start_time': (0,6),
                       'stop_time': (8,14),
                       'event': (17,35),
                       'start_height': (37,52),
                       'start_hdg': (55,58),
                       'comment': (59,-1),
                       'stop_height': None,
                       'stop_hdg': None,
                       'start_lat': None,
                       'stop_lat': None,
                       'start_lon': None,
                       'stop_lon': None}

        # Update any default column widths with user-supplied kwargs
        for k,v in kwargs.items():
            _fieldnames[k].update(v)

        # Read metadata from top of text file
        metaregex = {'flightnum': '^flight.*?(?P<flightnum>[a-z]\d{3})', # flight number
                     'date': '^date:.*?(?P<date>.*)',                    # date, any format
                     'project': '^project:.*?(?P<project>.*)',           # project name
                     'location': '^location:.*?(?P<location>.*)'}        # location
        _metadata = {}

        with open(self.path, 'r') as _txt:
            for row in range(metarows-2):
                _metaline = _txt.readline()
                for k, reg in metaregex.items():
                    metaval = re.search(reg, _metaline, flags=re.I)
                    if metaval is None:
                        continue
                    else:
                        _ = _metadata.setdefault(k, metaval.group(k))
                        break

        if fltdate != None:
            date = pd.to_datetime(fltdate, errors='coerce').date()
        else:
            date = pd.to_datetime(_metadata['date'], errors='coerce').date()

        if pd.isnull(date) == True:
            raise ValueError('Invalid date format given in flight summary.')

        # Read the main tablulated data
        with open(self.path, 'r') as _txt:
            table = pd.read_fwf(_txt,
                skiprows=metarows,
                names=[f for f,t in _fieldnames.items() if t != None],
                colspecs=[t for t in _fieldnames.values() if t != None],
                skipinitialspace=True,
                index_col=False,
                parse_dates=['start_time', 'stop_time'],
                date_parser=lambda t: datetime.datetime.combine(
                                date,
                                pd.to_datetime(t, format='%H%M%S').time()))

        # Convert 'comment' NaNs to empty strings
        table['comment'] = table['comment'].fillna('')

        # Convert df to list of dictionaries
        ### NOTE: creates regular dict not OrderedDict as _get_csv() does
        ### there doesn't seem a lot of point to an ordere dict but if one is
        ### required then do;
        ###     from collections import OrderedDict
        ###     ret_list = table.to_dict(orient='records', into=OrderedDict)
        ret_list = table.to_dict(orient='records')

        self.metadata = _metadata

        return sorted(ret_list, key=lambda x: x['start_time'])


    def _get_time_event(self, time, within=pd.Timedelta(seconds=60)):
        """ Returns event/s at exactly time or within tolerance

        Will return event/s that are current at `time`, so any ongoing events
        (those with both start and end times) that cover `time` will be
        returned. This means that if `within` is set too large then adjacent
        runs/profiles may both be returned.

        Args:
            time (:obj:`str` or :obj:`datetime`): Time to search for in flight
                summary. If a string will be parsed by `pd.to_datetime`.
            within (:obj:`str` or :obj:`timedelta`): Tolerance used in search.
                If a string will be parsed by `pd.to_timedelta`.

        Returns:
            List of dictionaries of events that cover or close to `time`.

        Raises:
            TypeError: If either `time` or `within` cannot be converted to
            datetime-like objects.
        """
        parse_err_msg = lambda s,t: \
            "Cannot convert time string '{}' to pd.{}".format(t, s)
        try:
            _time = pd.to_datetime(time, errors='coerce') #, utc=True)
        except TypeError as err:
            raise err(parse_err_msg('Datetime',time))
        else:
            # Have coerced to NaT as cannot catch parser.ParserError :-/
            if pd.isna(_time):
                raise TypeError(parse_err_msg('Datetime',time))

        if within == None:
            within=pd.Timedelta(seconds=60)
        try:
            _within = pd.to_timedelta(within, errors='coerce')
        except TypeError as err:
            raise err(parse_err_msg('Timedelta',within))
        else:
            # Have coerced to NaT as cannot catch parser.ParserError :-/
            if pd.isna(_within):
                raise TypeError(parse_err_msg('Timedelta',within))

        def in_time(e):
            if e['stop_time'] == None:
                return abs(e['start_time'] - _time) <= _within
            else:
                return (e['start_time']-_within) <= _time <= (e['stop_time']+_within)

        return [event for event in self.get() if in_time(event)]


    def _get_event_time(self, item):
        """ Returns (start,stop), or (start,None), times of event.

        Args:
            item (:obj:`str`): Event name to search for in flight summary.

        Returns:
            Tuple of start and end times of `item` event. End time will be
            `None` if item is a single point event.

        Raises:
            KeyError: If item is not found in flight summary.
        """
        for event in self.get():
            if event['event'].lower() == item.lower():
                return (event['start_time'],event['stop_time'])

        raise KeyError('Event, {}, not found.'.format(item))


    def get(self):
        """ Returns the flight summary as a list of dictionaries for each event

        """
        return self._get()()


    def find(self, event):
        """
        ??
        """
        pass
