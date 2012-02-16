from datetime import timedelta, datetime
import time
import times
import json
from types import *

TIME_MATRIX = {
    'seconds': 1,
    'minutes': 1,
    'hours': 3600,
    'days': 86400,
    'weeks': 86400*7,
    'years': 86400*365.25
        }

class whale_cache(object):
    """
    Decorator that caches a function's return value each time it is called.
    If called later with the same arguments, the cached value is returned, and
    not re-evaluated.
    """
    def __init__(self, func):
        self.func = func
        self.cache = None

    def get_cache(self):
        from whale import Whale
        self.cache = Whale.whale_driver()
        return self.cache

    def keyify(self, args, kwargs):
        return json.dumps((args, kwargs))

    def __call__(self, *args, **kwargs):
        from whale import Whale
        if len(args) and args[0] == Whale or issubclass(args[0], Whale):
            args = args[1:]
        clear_cache = kwargs.pop('unmemoize', False)
        self.get_cache()
        if 'period' in kwargs:
            kwargs['period'] = str(kwargs['period'])
            ttl = int(kwargs['period'].split('x')[0]) * 2
        else:
            ttl = 60

        key_name = self.keyify(args, kwargs)

        if clear_cache:
            self.cache.delete(key_name)

        try:
            return json.loads(self.cache[key_name])
        except KeyError:
            value = self.func(Whale, *args, **kwargs)
            self.cache[key_name] = json.dumps(value)
            self.cache.expire(key_name, ttl)
            return value
        except TypeError:
            # uncachable -- for instance, passing a list as an argument.
            # Better to not cache than to blow up entirely.
            return self.func(Whale, *args, **kwargs)

    def __repr__(self):
        """Return the function's docstring."""
        return self.func.__doc__

    def __get__(self, obj, objtype):
        """Support instance methods."""
        import functools
        return functools.partial(self.__call__, obj)

JS_URL = '/js.js'
JS_TAG = '<script type="text/javascript" src="%s"></script>' % JS_URL


def nested_dict_to_list_of_keys(d):
    for k, f in d.iteritems():
        yield [k]
        if type(f) is dict:
            for n in nested_dict_to_list_of_keys(f):
                yield [k] + n
        else:
            yield [k, f]


def datetimeIterator(from_date=None, to_date=None, use_utc=True, delta=timedelta(days=1)):
    if not from_date:
        if use_utc:
            from_date = times.now()
        else:
            from_date = datetime.now()
    while to_date is None or from_date <= to_date:
        yield from_date
        from_date = from_date + delta
    return


def to_flot_time(dt):
    return int(float(time.mktime(dt.timetuple())) * 1000)


def curry_instance_attribute(attr, func_name, instance, with_class_name=False):
    """ Curries the named attribute to the named function
    >>> class Person():
    ...     def __init__(self, name):
    ...         self.name = name
    ...         curry_instance_attribute('name', 'print_record', self)
    ...         curry_instance_attribute('upper_name', 'print_record_upper', self)
    ...     @classmethod
    ...     def print_record(cls, name):
    ...         print 'Person',name
    ...     @classmethod
    ...     def print_record_upper(cls, name):
    ...         print 'PERSON',name
    ...     def upper_name(self):
    ...         return self.name.upper()
    >>> Person.print_record('bob')
    Person bob
    >>> p=Person('jane')
    >>> p.print_record()
    Person jane
    >>> p.print_record_upper()
    PERSON JANE
    """

    func = getattr(instance, func_name)

    def curried(self, *args, **kwargs):
        pass_attr = getattr(self, attr)
        # Can also be callable
        if hasattr(pass_attr, '__call__'):
            pass_attr = pass_attr()
        if with_class_name:
            pass_attr = '_'.join(map(str, [instance.__class__.__name__, pass_attr]))
        return func(pass_attr, *args, **kwargs)

    setattr(instance, func_name,
            MethodType(curried, instance, instance.__class__))


def period_points(self, metric=False, period_str='60x86400',
        start=False, end=False):
    from datetime import timedelta
    from graphs import GraphPeriod
    interval, length = period_str.split('x')
    period = GraphPeriod(interval, length)
    if not start:
        start = times.now() - (timedelta(seconds=int(interval)) * 15)
    for dt in period.datetimes(start, end):
        dtf = '%sx%s-%s' % (interval, length, period.flatten_str(dt))
        v = {}
        if dtf in self.points:
            v = self.points[dtf].values
        if metric:
            if metric in v:
                v = v[metric]
            else:
                v = 0
        yield dt, v

if __name__ == "__main__":
    import doctest
    doctest.testmod()
