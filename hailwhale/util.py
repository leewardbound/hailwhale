from datetime import timedelta, datetime
import time
import times
import json
from types import *


DELIM = '||'

def try_loads(arg):
    if isinstance(arg, basestring) and arg and arg[0] in ['[', '{', '"', "'"]:
        try:
            arg = json.loads(arg.replace("'", '"'))
        except Exception as e:
            pass
    if isinstance(arg, list):
        if len(arg) == 1:
            return arg[0]
    return arg


def maybe_dumps(arg, dump_dicts=True):
    if isinstance(arg, basestring):
        arg = try_loads(arg)
    if isinstance(arg, basestring):
        return unicode(arg.decode('UTF-8'))
    if isinstance(arg, list):
        if len(arg) == 1:
            return maybe_dumps(arg[0])
        return json.dumps(map(maybe_dumps, arg))
    if isinstance(arg, dict):
        d = dict([
            (maybe_dumps(k), maybe_dumps(v, dump_dicts=False)) for k, v in arg.items()
            ])
        if dump_dicts:
            d = json.dumps(d)
        return d
    return json.dumps(arg)


def parent(sub):
    sub = try_loads(sub)
    if sub == '_':
        return None
    elif isinstance(sub, list) and len(sub) > 1:
        return sub[:-1]
    else:
        return '_'


def keyify(*args):
    json_args = map(maybe_dumps, map(try_loads, args))
    return DELIM.join([arg if arg not in
        [None, False, '[null]', [], ['_'], '', '""', '"_"', '\"\"', '["_"]']
        else '_' for arg in json_args])

TIME_MATRIX = {
    'seconds': 1,
    'minutes': 1,
    'hours': 3600,
    'days': 86400,
    'weeks': 86400 * 7,
    'years': 86400 * 365.25
        }


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

    def hailwhale_pk_curried(self, *args, **kwargs):
        pass_attr = getattr(self, attr)
        # Can also be callable
        if hasattr(pass_attr, '__call__'):
            pass_attr = pass_attr()
        if with_class_name:
            pass_attr = [instance.__class__.__name__, '%s' % pass_attr]
        return func(pass_attr, *args, **kwargs)

    setattr(instance, func_name,
            MethodType(hailwhale_pk_curried, instance, instance.__class__))


def curry_related_dimensions(attr, func_name, instance, with_class_name=False):
    func = getattr(instance, func_name)

    def related_curry_func(self, relation, *args, **kwargs):
        pk = getattr(self, attr)
        # Can also be callable
        if hasattr(pk, '__call__'):
            pk = pk()
        pk = with_class_name and [self.class_name(), str(pk)] or [str(pk)]
        if isinstance(relation, basestring):
            relation = getattr(self, relation)
        rel_type, rel_pk = relation.class_name(), str(relation.id)
        nest = lambda d: d and {rel_type: {rel_pk: d}} or {rel_type: rel_pk}

        if 'dimension' in kwargs:
            kwargs['dimension'] = nest(kwargs['dimension'])
        else:
            kwargs['dimensions'] = nest(kwargs.pop('dimensions', None))
        return func(*args, **kwargs)

    setattr(instance, func_name + '_related',
            MethodType(related_curry_func, instance, instance.__class__))


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
        from periods import Period
        if len(args) and args[0] == Whale or issubclass(args[0], Whale):
            args = args[1:]
        clear_cache = kwargs.pop('unmemoize', False)
        self.get_cache()
        if 'period' in kwargs:
            p = Period.get(kwargs['period'])
            kwargs['period'] = str(p)
            ttl = int(p.interval) / 5
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


if __name__ == "__main__":
    import doctest
    doctest.testmod()
