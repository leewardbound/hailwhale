from datetime import datetime, timedelta, date
import time
import times
PERIODS = [
{'name': 'Last year, by 14 days', 
    'length': 3600 * 24 * 365,
    'interval': 3600 * 24 * 14,
    'nickname': 'year'},
{'name': 'Last 30 days, by day', 'length': 3600 * 24 * 30, 'interval': 3600 * 24,
    'nickname': 'thirty'},
{'name': 'Last week, by 6 hours', 'length': 3600 * 24 * 7, 'interval': 3600 * 6,
    'nickname': 'seven'},
{'name': 'Last day, by hour', 'length': 3600 * 24, 'interval': 3600,
    'nickname': '24h'},
{'name': 'Last 6 hours, by 15 minutes', 'length': 3600 * 6, 'interval': 60 * 15},
{'name': 'Last hour, by 2 minutes', 'length': 3600, 'interval': 60 * 2,
    'nickname': 'hour'},
{'name': 'Last 5 minutes, by 10 seconds', 'length': 300, 'interval': 10,
    'nickname': 'fivemin'},
]
class Period(object):
    def __init__(self, interval, length, name=False, nickname=False):
        self.interval = int(interval)
        self.length = int(length)
        self.name = name
        self.nickname = nickname
    @classmethod
    def get_points(cls, period, at=None):
        ats = False
        if period == 'ytd':
            period = 'year'
            period = cls.get(period)
            start = datetime.now().replace(month=1, day=1,hour=0,minute=0,second=0)
            ats = period.datetimes_strs(start=start)
        if period == 'mtd':
            period = 'thirty'
            period = cls.get(period)
            start = datetime.now().replace(day=1, hour=0, minute=0, second=0)
            ats = period.datetimes_strs(start=start)
        if period == 'wtd':
            period = 'thirty'
            period = cls.get(period)
            day = datetime.now().day
            start = datetime.now().replace(day=max(1, 1 + (day - (day%7))), hour=0, minute=0, second=0)
            ats = period.datetimes_strs(start=start)
        if period == 'today':
            period = 'thirty'
            period = cls.get(period)
            start = datetime.now().replace(hour=0, minute=0, second=0)
            ats = period.datetimes_strs(start=start)
        if period == 'yesterday':
            period = 'thirty'
            period = cls.get(period)
            end = datetime.now().replace(hour=0, minute=0, second=0)
            start = end - timedelta(1)
            ats = period.datetimes_strs(start=start, end=end)
        if period == 'seven':
            period = 'thirty'
            period = cls.get(period)
            start = datetime.now().replace(hour=0, minute=0, second=0) - timedelta(7)
            ats = period.datetimes_strs(start=start)

        period = cls.get(period)
        if not ats and not at:
            ats = period.datetimes_strs()
        elif not ats:
            ats = [period.flatten_str(at)]
        return period, list(ats)

    def start(self):
        dt= (times.now() -
                timedelta(seconds=self.length))
        if self.interval < 60:
            interval_seconds = self.interval
        else: interval_seconds = 60
        if self.interval < 3600:
            interval_minutes = (self.interval - interval_seconds)/60
        else: interval_minutes = 60
        if self.interval < 3600*24:
            interval_hours = (self.interval - interval_seconds -
                    (60*interval_minutes))/3600
        else:
            interval_hours = 24
        if interval_hours == 0: interval_hours = 1
        if interval_minutes == 0: interval_minutes = 1
        return dt.replace(
            microsecond = 0,
            second = (dt.second - dt.second%interval_seconds),
            minute = (dt.minute - dt.minute%interval_minutes),
            hour = (dt.hour - dt.hour%interval_hours),)
    def delta(self):
        return timedelta(seconds=self.interval)
    @staticmethod
    def format_dt_str(t):
        return t.strftime('%a %b %d %H:%M:%S %Y')
    @staticmethod
    def parse_dt_str(t):
        try:
            return datetime.strptime(t, '%a %b %d %H:%M:%S %Y')
        except ValueError:
            return None

    def datetimes(self, start=False, end=False):
        from util import datetimeIterator
        in_range = lambda dt: (not start or start <= dt) and (
            not end or end >= dt)
        return (dt for dt in datetimeIterator(
            self.start(), times.now(),
            delta=self.delta()) if in_range(dt))

    def datetimes_strs(self, start=False, end=False):
        return (Period.format_dt_str(dt) for dt in
                self.datetimes(start=start, end=end))

    def flatten(self, dtf=None):
        if not dtf:
            dtf = datetime.now()
        if type(dtf) in (str, unicode):
            dtf = self.parse_dt_str(dtf)
        if not dtf:
            return False
        diff_delta = dtf - self.start()
        diff = diff_delta.seconds + (diff_delta.days * 86400)
        if diff < 0:
            return False
        p = int(diff / self.interval)
        flat = (self.start() + timedelta(seconds=p * self.interval)).replace(microsecond=0)
        return flat

    def flatten_str(self, dtf):
        f = self.flatten(dtf)
        if not f:
            return False
        return self.format_dt_str(f)

    def __unicode__(self):
        return '%dx%d' % (self.interval, self.length)

    def __str__(self):
        return '%dx%d' % (self.interval, self.length)

    @staticmethod
    def all_sizes():
        return PERIOD_OBJS

    @staticmethod
    def all_sizes_dict():
        return dict(map(lambda p: ('%sx%s' % (p.interval, p.length), p),
            Period.all_sizes()))

    @staticmethod
    def get(name=None):
        if isinstance(name, Period):
            return name
        if name and name in PERIOD_NICKS:
            return PERIOD_NICKS[str(name)]
        if not name:
            name = Period.default_size()
        return Period.all_sizes_dict()[str(name)]

    @staticmethod
    def default_size():
        return str(Period.all_sizes()[1])

    def friendly_name(self):
        return self.name if self.name else '%sx%s' % (
                self.interval, self.length)

PERIOD_OBJS = []
PERIOD_NICKS = {} 
for p in PERIODS:
    period = Period(p['interval'], p['length'], p['name'], p.get('nickname', None))
    PERIOD_OBJS.append(period)
    if 'nickname' in p:
        PERIOD_NICKS[p['nickname']] = period
DEFAULT_PERIODS = Period.all_sizes()
