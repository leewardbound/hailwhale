import time
import random
import json
import itertools
import collections

from redis import Redis
from collections import defaultdict
from itertools import product
from datetime import datetime

from util import to_flot_time, curry_instance_attribute
from util import nested_dict_to_list_of_keys
from periods import DEFAULT_PERIODS, Period

DELIM = '||'

"""
Ahoy, traveler!
You're a long way from home!
Go back now?
(y/n)> n
Are you sure?
(y/n)> y
A nearby sign reads, 
    """ + "" + "" + "" + "" + "" + """
    Here there be dragons!
     This is the belly of the beast, 
     the heart of the whale, 
     this thing pulls the
     wings right off of bees! 
     Live fucking bees! 
     While they're flying and shit!
    DON'T SAY WE DIDN'T WARN YOU
    """ + "" + "" + "" + "" + "" + """
                """+''+"""    *
      '  *      """+''+"""    ' *
       \'  *    """+''+""" *  \ ' 
        '  '    """+''+""" '   *
      """"""""""""""""""""""""""""""
Brave traveler that you are, you cut the sign in half!
With an appetite for insanity, you dive deep into the sauce,
hacking and slashing with your +3 caffiene enhanced coffeemug-of-glory.

Take an advil, or three. It's going to be a long day.

    <3 leeward

P.S. don't trust the comments --
    they are as sparse as they are outdated

"""

def try_loads(arg):
    try: return json.loads(arg)
    except: return arg

def keyify(*args):
    json_args = [json.dumps(arg) if not isinstance(arg, basestring) else arg
            for arg in map(try_loads, args)]
    return DELIM.join([arg if arg not in 
        [None, False, '[null]', [], ['_'], '', '""', '"_"', '\"\"', '["_"]']
        else '_' for arg in json_args ])

class WhaleRedisDriver(Redis):
    def __init__(self, *args, **kwargs):
        super(WhaleRedisDriver, self).__init__(*args, **kwargs)
        self._added_dimensions = collections.defaultdict(list)
        self._added_subdimensions = collections.defaultdict(list)

    def store(self, pk, dimension, metric, period, dt, count):
        # Keep a list of graphs per pk
        key = keyify(pk, dimension, period, metric)
        # Store pk dimensions
        dimension_key = keyify(pk,'dimensions')
        dimension_json = keyify(dimension)
        if not dimension_json in self._added_dimensions[dimension_key]:
            self.sadd(dimension_key,dimension_json)
            self._added_dimensions[dimension_key].append(dimension_json)
        # Store dimensional subdimensions
        if len(dimension) > 1:
            parent_dimension = dimension[:-1]
        else: parent_dimension = '_'
        if(dimension != '_'):
            subdimension_key = keyify(pk,'subdimensions',parent_dimension)
            if not dimension_json in self._added_subdimensions[subdimension_key]:
                self.sadd(subdimension_key, dimension_json)
                self._added_subdimensions[subdimension_key].append(dimension_json)
        return self.hincrby(key, dt, int(count))
    def retrieve(self, pk, dimensions, metrics, period='all', dt=None):
        pk = json.dumps(pk)
        nested = defaultdict(dict)
        to_i = lambda n: int(n) if n else 0
        if period=='all': dt='time'
        for dimension in iterate_dimensions(dimensions):
            for metric in metrics:
                if not isinstance(dimension, basestring):
                    dimension = json.dumps(dimension) 
                elif dimension == '"_"':
                    dimension = '_'
                hash_key = keyify(pk, dimension, period, metric)
                value_dict = self.hgetall(hash_key)
                if period=='all' and dt == 'time':
                    nested[dimension][metric] = float(value_dict.get('time', 0))
                else: nested[dimension][metric] = dict([
                        (k, float(v)) for k,v in value_dict.items()])
        return dict(nested)

class Whale(object):
    whale_driver_class = WhaleRedisDriver
    whale_driver_settings = {}
    def curry_whale_instance_methods(self, attr='id'):
        if hasattr(self, attr):
            curry_instance_attribute(attr, 'plotpoints', self)
            curry_instance_attribute(attr, 'totals', self)
            curry_instance_attribute(attr, 'count_now', self)
            curry_instance_attribute(attr, 'reset', self)

    @classmethod
    def whale_driver(cls):
        if not hasattr(cls, '_whale_driver'):
            cls._whale_driver = cls.whale_driver_class(**cls.whale_driver_settings)
        return cls._whale_driver

    @classmethod
    def plotpoints(cls, pk, dimensions=None, metrics=None,
            depth=0, period=None, flot_time=False, points_type=dict):
        metrics = metrics or ['hits',]
        if isinstance(metrics, basestring): metrics = [metrics]
        period = period or Period.default_size()
        sparse = cls.whale_driver().retrieve(pk,dimensions,metrics, period=period)
        nonsparse = defaultdict(dict)
        for dimensions, metrics in sparse.items():
            for metric, points in metrics.items():
                dts = Period(*period.split('x')).datetimes_strs()
                nonsparse[dimensions][metric] = []
                for dt in dts:
                    if flot_time: dt = to_flot_time(Period.parse_dt_str(dt))

                    value = points[dt] if dt in points else 0
                    nonsparse[dimensions][metric].append([dt,float(value)])
                nonsparse[dimensions][metric] = points_type(nonsparse[dimensions][metric])
        if depth > 0:
            for sub in cls.get_subdimensions(pk,dimensions):
                nonsparse = dict(nonsparse.items() +
                    cls.plotpoints(pk,sub,metrics,depth-1,period).items())
        return nonsparse
    @classmethod
    def ratio_plotpoints(cls, pk, numerator_metric, denomenator_metric='hits',
            dimensions=None, depth=0, period=None, flot_time=False, points_type=dict):
        top,bot = numerator_metric, denomenator_metric  
        pps = cls.plotpoints(pk, dimensions, [top,bot], depth=depth, period=period,
            flot_time=flot_time, points_type=points_type)
        def ratio_func(tup):
            dim, mets = tup
            return (dim, dict([(dt,
                    denom and (mets[top][dt]/denom) or 0)
                                    for dt,denom in mets[bot].items()]))
        return dict(map(ratio_func, pps.items()))

    @classmethod
    def totals(cls, pk, dimensions=None, metrics=None):
        metrics = metrics or ['hits',]
        if not isinstance(metrics, list): metrics = [metrics,]
        d = {}
        for p in DEFAULT_PERIODS: 
            p_data = cls.whale_driver().retrieve(
                pk,dimensions,metrics,period=str(p))
            p_totals = dict()
            for dim, mets in p_data.items():
                p_totals[dim] = dict()
                for met, vals in mets.items():
                    p_totals[dim][met] = sum([
                        v for k,v in vals.items()
                        if p.flatten(k)])
            d[str(p)] = p_totals
        d['alltime'] = cls.whale_driver().retrieve(
                pk, dimensions, metrics, period='all')
        return d

    @classmethod
    def reset(cls, pk, dimensions=None, metrics=None):
        r= cls.whale_driver().reset(
                pk,dimensions,metrics)
        return r

    @classmethod
    def cleanup(cls):       
        ps = dict([(str(p), p) for p in DEFAULT_PERIODS])
        r = cls.whale_driver()
        keys = r.keys('*||*||*||*')
        for k in keys:
            try: val = r.hgetall(k)
            except: 
                r.delete(k)
                continue
            this_p = k.split('||')[2]
            if this_p == 'all': continue
            if not this_p in ps:
                r.delete(k)
                continue
            deleted = 0
            for dt, num in val.items():
                if not ps[this_p].flatten(dt):
                    r.hdel(k, dt)
                    deleted += 1
            # Cleanup empty key
            if (len(val) - deleted) == 0:
                r.delete(k)
                print 'Key empty, deleting --',k
            elif deleted > 0:
                print 'Deleted',deleted,'old keys from',k

    @classmethod
    def get_subdimensions(cls, pk, dimension='_'):
        if dimension == ['_']: dimension = '_'
        return map(lambda s: map(str, json.loads(s)),
                cls.whale_driver().smembers(keyify(pk,'subdimensions',
                    dimension)))

    @classmethod
    def all_subdimensions(cls, pk, dimension='_'):
        subdimensions = []
        for d in cls.get_subdimensions(pk, dimension):
            subdimensions += cls.all_subdimensions(pk, d)
        if dimension: 
            subdimensions.append(dimension)
        return subdimensions

    @classmethod
    def count_now(cls, pk, dimensions, metrics=None, at=False):
        """ Immediately count a hit, as opposed to logging it into Hail"""
        r=cls.whale_driver()
        periods = DEFAULT_PERIODS

        if isinstance(at, basestring):
            try:
                if ':' in at: at = datetime.strptime(at, '%c')
                else: at = float(at)
            except Exception as e: print e
        if not metrics: metrics = list()
        if type(metrics) == list:
            metrics = dict([(k,1) for k in metrics])
        metrics['hits'] = 1
        # Dimensions: {a: 5, b: {x: 1, y: 2}} --> will increment each of: 
        # [_], (overall)
        # [a],
        # [a, 5], 
        # [b], 
        # [b, x], 
        # [b, x, 1],
        # [b, y], 
        # [b, y, 2]
        for dimension, (period, dt, metric, i) in itertools.product(
            iterate_dimensions(dimensions)+['_'],
                        generate_increments(metrics, periods, at)):
            cls._whale_driver.store(pk, dimension, metric, period, dt, i)

    @classmethod
    def crunch(cls, pk, dimension='_', metric='value', period='alltime'):
        divide = None
        d_k=keyify(dimension)
        if isinstance(metric, tuple):
            if len(metric) != 2:
                raise Exception("Crunch can take up to two dimensions")
            metric, divide = metric
        total = cls.totals(pk,dimension,metric)[period][d_k][metric]
        if divide: 
            divide_total = cls.totals(pk, dimension, divide)[period][d_k][divide]
            divided_total = divide_total and ( total / divide_total ) or 0
        print total, divided_total




def iterate_dimensions(dimensions):
    if not dimensions: dimensions = '_' 
    if isinstance(dimensions, dict):
        dimensions = list(nested_dict_to_list_of_keys(dimensions))
    elif isinstance(dimensions, basestring):
        dimensions = [dimensions, ]
    elif isinstance(dimensions, list) and len(dimensions) and not isinstance(dimensions[0], list):
        dimensions = [dimensions, ]
    return dimensions

def generate_increments(metrics, periods=False, at=False):
    periods = periods or DEFAULT_PERIODS
    observations = set()
    at = at or datetime.now()
    for period in periods:
        dt = period.flatten_str(at)
        if not dt: continue
        observations.add( (period,dt) )
    rr = [ (str(period),dt,metric,incr_by)
            for (period,dt) in observations
            for metric, incr_by in metrics.items()]
    for metric, incr_by in metrics.items():
        rr.append( ('all','time',metric,incr_by) )
    return rr
