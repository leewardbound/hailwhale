from datetime import datetime, timedelta, date
import time
import times
import re
PERIODS = [
{'name': 'Last 3 years, by month', 
    'length': '3y',
    'interval': '1mo',
    'nickname': 'monthly'},
{'name': 'Last year, by week', 
    'length': '1y',
    'interval': '1w',
    'nickname': 'year'},
{'name': 'Last 30 days, by day', 'length': '1mo', 'interval': '1d',
    'nickname': 'thirty'},
{'name': 'Last week, by 6 hours', 'length': '1w', 'interval': '6h',
    'nickname': 'seven'},
{'name': 'Last day, by hour', 'length': '1d', 'interval': '1h',
    'nickname': '24h'},
{'name': 'Last hour, by 1 minutes', 'length': '1h', 'interval': '1m',
    'nickname': 'hour'},
{'name': 'Last 5 minutes, by 10 seconds', 'length': '5m', 'interval': '10s',
        'nickname': 'fivemin'}
]

UnitMultipliers = {
  'seconds' : 1,
  'minutes' : 60,
  'hours' : 3600,
  'days' : 86400,
  'weeks' : 86400 * 7,
  'months' : 86400 * 31,
  'years' : 86400 * 365
}


def getUnitString(s):
  if 'seconds'.startswith(s): return 'seconds'
  if 'minutes'.startswith(s): return 'minutes'
  if 'hours'.startswith(s): return 'hours'
  if 'days'.startswith(s): return 'days'
  if 'weeks'.startswith(s): return 'weeks'
  if 'months'.startswith(s): return 'months'
  if 'years'.startswith(s): return 'years'
  raise ValueError("Invalid unit '%s'" % s)

def parseUnit(unit):
    if str(unit).isdigit():
        return int(unit) * UnitMultipliers[getUnitString('s')]
    unit_re = re.compile(r'^(\d+)([a-z]+)$')
    match = unit_re.match(str(unit))
    if match:
      unit = int(match.group(1)) * UnitMultipliers[getUnitString(match.group(2))]
    else:
      raise ValueError("Invalid unit specification '%s'" % unit)
    return unit

def parseRetentionDef(retentionDef):
  (precision, points) = retentionDef.strip().split(':')
  precision = parseUnit(precision)

  if points.isdigit():
    points = int(points)
  else:
    points = parseUnit(points) / precision

  return (precision, points)

class Period(object):
    def __init__(self, interval, length, name=False, nickname=False):
        self.interval = str(interval)
        self.length = str(length)
        self.name = name
        self.nickname = nickname
    def getUnits(self):
        return parseUnit(self.interval), parseUnit(self.length)
    @classmethod
    def get_days(cls, period, at=None, tzoffset=None):
        ats = False
        period = str(period)
        if '|' in period:
            period, tzoffset = period.split('|')
        if period == 'ytd':
            period = 'year'
            period = cls.get(period)
            start = convert(times.now(), tzoffset).replace(month=1,
                    day=1,hour=0,minute=0,second=0, microsecond=0)
            ats = period.datetimes_strs(start=start, tzoffset=tzoffset)
        if period == 'mtd':
            period = 'thirty'
            period = cls.get(period)
            start = convert(times.now(), tzoffset).replace(day=1, hour=0,
                    minute=0, second=0, microsecond=0)
            ats = period.datetimes_strs(start=start, tzoffset=tzoffset)
        if period == 'wtd':
            period = 'thirty'
            period = cls.get(period)
            start = convert(times.now(), tzoffset).replace(hour=0, minute=0,
                    second=0, microsecond=0)
            start = start - timedelta(start.weekday() + 2)
            ats = period.datetimes_strs(start=start, tzoffset=tzoffset)
        if period in ['today', 'hours']:
            period = 'thirty'
            period = cls.get(period)
            start = convert(times.now(), tzoffset).replace(hour=0, minute=0,
                    second=0, microsecond=0)
            ats = period.datetimes_strs(start=start, tzoffset=tzoffset)
        if period == 'yesterday':
            period = 'thirty'
            period = cls.get(period)
            end = convert(times.now(), tzoffset).replace(hour=0, minute=0,
                    second=0, microsecond=0)
            start = end - timedelta(1)
            end = end - timedelta(seconds=1)
            ats = period.datetimes_strs(start=start, end=end, tzoffset=tzoffset)
        if period == 'seven':
            period = 'thirty'
            period = cls.get(period)
            start = convert(times.now(), tzoffset).replace(hour=0, minute=0,
                    second=0, microsecond=0) - timedelta(7)
            ats = period.datetimes_strs(start=start, tzoffset=tzoffset)
        if '-' in str(period):
            start_s, end_s = period.split('-')
            period = 'thirty'
            period = cls.get(period)
            end = datetime.strptime(end_s, '%m/%d/%Y').replace(hour=0, minute=0,
                    second=0, microsecond=0)+timedelta(1)-timedelta(seconds=1)
            start = datetime.strptime(start_s, '%m/%d/%Y').replace(hour=0, minute=0, second=0, microsecond=0)
            ats = period.datetimes_strs(start=start, end=end, tzoffset=tzoffset)


        period = cls.get(period)
        if not ats and not at:
            ats = period.datetimes_strs(tzoffset=tzoffset)
        elif not ats:
            ats = [period.flatten_str(convert(at, tzoffset))]
        return period, list(ats), tzoffset

    def start(self):
        interval, length = self.getUnits()
        dt= (times.now() -
                timedelta(seconds=length))
        if interval < 60:
            interval_seconds = interval
        else: interval_seconds = 60
        if interval < 3600:
            interval_minutes = (interval - interval_seconds)/60
        else: interval_minutes = 60
        if interval < 3600*24:
            interval_hours = (interval - interval_seconds -
                    (60*interval_minutes))/3600
        else:
            interval_hours = 24
        if interval_hours == 0: interval_hours = 1
        if interval_minutes == 0: interval_minutes = 1
        new_start = dt.replace(
            microsecond = 0,
            second = (dt.second - dt.second%interval_seconds),
            minute = (dt.minute - dt.minute%interval_minutes),
            hour = (dt.hour - dt.hour%interval_hours),)
        if interval >= (3600*24*30):
            new_start = new_start.replace(day=1)
        return new_start
    @staticmethod
    def format_dt_str(t):
        return t.strftime('%a %b %d %H:%M:%S %Y')
    @staticmethod
    def parse_dt_str(t):
        try:
            return datetime.strptime(t, '%a %b %d %H:%M:%S %Y')
        except ValueError:
            return None

    def datetimes(self, start=False, end=False, tzoffset=None):
        from dateutil import rrule
        from util import datetimeIterator
        in_range = lambda dt: (not start or start <= dt) and (
            not end or end >= dt)
        use_start = start or self.start()
        use_end = end or convert(times.now(), tzoffset)
        interval, length = self.getUnits()
        if interval >= 3600*24*30:
            rule = rrule.MONTHLY
            step = interval / (3600*24*30)
        elif interval >= 3600*24*7:
            rule = rrule.WEEKLY
            step = interval / (3600*24*7)
        elif interval >= 3600*24:
            rule = rrule.DAILY
            step = interval / (3600*24)
        elif interval >= 3600:
            rule = rrule.HOURLY
            step = interval / 3600
        elif interval >= 60:
            rule = rrule.MINUTELY
            step = interval / 60
        else:
            rule = rrule.SECONDLY
            step = interval
        dts = rrule.rrule(rule, dtstart=use_start, until=use_end, interval=step)
        return dts

    def datetimes_strs(self, start=False, end=False, tzoffset=None):
        return (Period.format_dt_str(dt) for dt in
                self.datetimes(start=start, end=end, tzoffset=tzoffset))

    def flatten(self, dtf=None):
        if not dtf:
            dtf = times.now()
        if type(dtf) in (str, unicode):
            dtf = self.parse_dt_str(dtf)
        dts = list(self.datetimes(end=dtf))
        flat = len(dts) and dts[-1] or False
        return flat

    def flatten_str(self, dtf):
        f = self.flatten(dtf)
        if not f:
            return False
        return self.format_dt_str(f)

    def __unicode__(self):
        return '%s:%s' % (self.interval, self.length)

    def __str__(self):
        return '%s:%s' % (self.interval, self.length)

    @staticmethod
    def all_sizes():
        return PERIOD_OBJS

    @staticmethod
    def all_sizes_dict():
        return dict(map(lambda p: ('%s:%s' % (p.interval, p.length), p),
            Period.all_sizes()))

    @staticmethod
    def get(name=None):
        if isinstance(name, Period):
            return name
        if name and name in PERIOD_NICKS:
            return PERIOD_NICKS[str(name)]
        if not name or name == 'None':
            name = Period.default_size()
        if str(name) in Period.all_sizes_dict():
            return Period.all_sizes_dict()[str(name)]
        try:
            return PERIOD_INTERVALS[parseUnit(name)]
        except:
            raise KeyError(name)

    @staticmethod
    def default_size():
        return str(Period.all_sizes()[1])
    @staticmethod
    def convert(tz, tzo):
        return convert(tz, tzo)

    def friendly_name(self):
        return self.name if self.name else '%s:%s' % (
                self.interval, self.length)

PERIOD_OBJS = []
PERIOD_NICKS = {} 
PERIOD_INTERVALS = {} 
for p in PERIODS:
    period = Period(p['interval'], p['length'], p['name'], p.get('nickname', None))
    PERIOD_OBJS.append(period)
    PERIOD_INTERVALS[parseUnit(p['interval'])] = period
    if 'nickname' in p:
        PERIOD_NICKS[p['nickname']] = period
        PERIOD_NICKS[p['interval']] = period
DEFAULT_PERIODS = Period.all_sizes()
def convert(tzs, tzoffset=None):
    if tzoffset == 'system':
        tzoffset = (time.timezone / -(60*60) * 100)
    if not tzoffset:
        return tzs
    elif isinstance(tzs, datetime):
        return tzs + timedelta(hours=float(tzoffset)/100)
    elif isinstance(tzs, basestring):
        return times.format(tzs, int(tzoffset))
    elif isinstance(tzs, int):
        return tzs + int(3600*float(tzoffset)/100)
    elif isinstance(tzs, list):
        return map(lambda tz: convert(tz, float(tzoffset)), tzs)
