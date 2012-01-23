from datetime import datetime, timedelta
import time
class Period(object):
    def __init__(self, interval, length, name=False):
        self.interval = int(interval)
        self.length = int(length)
        self.name = name
    def start(self):
        dt= (datetime.now() -
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
            second = (dt.second - dt.second%interval_seconds),
            minute = (dt.minute - dt.minute%interval_minutes),
            hour = (dt.hour - dt.hour%interval_hours),)
    def delta(self):
        return timedelta(seconds=self.interval)
    @staticmethod
    def format_dt_str(t):
        return t.strftime('%c')
    @staticmethod
    def parse_dt_str(t):
        return datetime.strptime(t, '%c')

    def datetimes(self, start=False, end=False):
        from util import datetimeIterator
        return (dt for dt in datetimeIterator(
            start or self.start(), 
            end or datetime.now(),
            delta=self.delta()))

    def datetimes_strs(self, start=False, end=False):
        return (Period.format_dt_str(dt) for dt in
                self.datetimes(start=start,end=end))
        
    def flatten(self, dtf):
        if type(dtf) in (str, unicode): dtf = self.parse_dt_str(dtf)
        diff_delta = dtf - self.start()
        diff = diff_delta.seconds + diff_delta.days*86400
        if diff < 0: return False
        p = int(diff / self.interval)
        flat = self.start() + timedelta(seconds=p*self.interval)
        return flat

    def flatten_str(self,dtf):
        f = self.flatten(dtf)
        if not f: return False
        return self.format_dt_str(f)

    def __unicode__(self):
      return '%dx%d'%(self.interval, self.length)

    def __str__(self):
      return '%dx%d'%(self.interval, self.length)

    @staticmethod
    def all_sizes():
        PERIODS = [
{'name': 'Last year, by 14 days', 'length': 3600*24*365, 'interval': 3600*24*14},
{'name': 'Last week, by 6 hours', 'length': 3600*24*7, 'interval': 3600*6},
{'name': 'Last day, by hour', 'length': 3600*24, 'interval': 3600},
{'name': 'Last 6 hours, by 15 minutes', 'length': 3600*6, 'interval': 60*15},
{'name': 'Last hour, by 2 minutes', 'length': 3600, 'interval': 60*2},
{'name': 'Last 5 minutes, by 10 seconds', 'length': 300, 'interval': 10},
]
        PERIOD_OBJS = []
        for p in PERIODS:
            period = Period(p['interval'], p['length'], p['name'])
            PERIOD_OBJS.append(period)
        return PERIOD_OBJS

    @staticmethod
    def all_sizes_dict():
        return dict(map(lambda p: ('%sx%s'%(p.interval,p.length),p),
            Period.all_sizes()))
    @staticmethod
    def get(name):
        return Period.all_sizes_dict()[name]
    @staticmethod
    def default_size():
        return str(Period.all_sizes()[-4])
        
    def friendly_name(self):
        return self.name if self.name else '%sx%s'%(
                self.interval, self.length)

DEFAULT_PERIODS = Period.all_sizes()
