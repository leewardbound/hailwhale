from datetime import timedelta, datetime
import time
from types import *

JS_URL='/js.js'
JS_TAG='<script type="text/javascript" src="%s"></script>'%JS_URL


def nested_dict_to_list_of_keys(d):
    for k,f in d.iteritems():
        yield [k,]
        if type(f) is dict:
            for n in nested_dict_to_list_of_keys(f):
                yield [k,] + n
        else: yield [k,f]

def datetimeIterator(from_date=None, to_date=None, use_utc=False, delta=timedelta(days=1)):
    if not from_date:
        if use_utc: from_date = datetime.utcnow()
        else: from_date = datetime.now()
    while to_date is None or from_date <= to_date:
        yield from_date
        from_date = from_date + delta
    return

def to_flot_time(dt):
    return float(time.mktime(dt.timetuple()))*1000

def curry_instance_attribute(attr, func_name, instance):
    """ Curries the named attribute to the named function
    >>> class Person():
    ...     def __init__(self, name):
    ...         self.name = name
    ...         curry_instance_attribute('name', 'print_record', self)
    ...     @classmethod
    ...     def print_record(cls, name):
    ...         print 'Person',name
    >>> Person.print_record('bob')
    Person bob
    >>> p=Person('jane')
    >>> p.print_record()
    Person jane
    """
    from types import MethodType
    func = getattr(instance, func_name)
    def curried(self, *args, **kwargs):
        return func(getattr(self, attr),
                *args, **kwargs)
    setattr(instance, func_name,
            MethodType(curried, instance, instance.__class__))

def period_points(self, metric=False, period_str='60x86400', 
        start=False, end=False):
    from datetime import timedelta
    from graphs import GraphPeriod
    interval, length = period_str.split('x')
    period = GraphPeriod(interval, length)
    if not start: start = datetime.now() - (
            timedelta(seconds=int(interval)) * 15)
    for dt in period.datetimes(start, end):
        dtf = '%sx%s-%s'%(interval, length, period.flatten_str(dt))
        v = {}
        if dtf in self.points: 
            v = self.points[dtf].values
        if metric:
            if metric in v: v = v[metric]
            else: v = 0
        yield dt, v

if __name__ == "__main__":
    import doctest
    doctest.testmod()
