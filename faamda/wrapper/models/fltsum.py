import csv
import datetime

import pandas as pd

from .abc import DataModel

__all__ = ['FltSumDataModel']


class FltSumDataModel(DataModel):
    """
    This is the data model for the models.CoreFltSumAccessor()

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
                if not item[var]:
                    item[var] = None
                    continue
                item[var] = datetime.datetime.strptime(
                    item[var], '%Y-%m-%d %H:%M:%S'
                )

            for var_t in ('start', 'stop'):
                for var_o in ('hdg', 'height', 'lat', 'lon'):
                    var = '{}_{}'.format(var_t, var_o)
                    if not item[var]:
                        item[var] = None
                        continue
                    item[var] = float(item[var])

        return sorted(ret_list[1:], key=lambda x: x['start_time'])


    def _get_txt(self):
        """  Reads .txt flight summary and puts contents into self.fltsum.

        """
        raise NotImplementedError


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
