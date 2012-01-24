import json
import itertools
import collections

from redis import Redis
from collections import defaultdict
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
               """ + '' + """    *
      '  *     """ + '' + """    ' *
       \'  *   """ + '' + """ *  \ '
        '  '   """ + '' + """ '   *
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
    try:
        return json.loads(arg)
    except:
        return arg

def maybe_dumps(arg):
    if isinstance(arg, basestring):
        return str(arg)
    if isinstance(arg, list) and len(arg) == 1:
        return maybe_dumps(arg[0])
    return json.dumps(arg)


def parent(sub):
    sub = try_loads(sub)
    if sub == '_':
        return None
    elif isinstance(sub, list) and len(sub) > 1:
        return sub[:-1]
    else:
        return '_'


>>>>>>> da565bc2dc7ba49e2dae4642cd925f481fc9e734
def keyify(*args):
    json_args = map(maybe_dumps, map(try_loads, args))
    return DELIM.join([arg if arg not in
        [None, False, '[null]', [], ['_'], '', '""', '"_"', '\"\"', '["_"]']
        else '_' for arg in json_args])


class WhaleRedisDriver(Redis):
    def __init__(self, *args, **kwargs):
        super(WhaleRedisDriver, self).__init__(*args, **kwargs)
        self._added_dimensions = collections.defaultdict(list)
        self._added_subdimensions = collections.defaultdict(list)

    def store(self, pk, dimension, metric, period, dt, count):
        # Keep a list of graphs per pk
        key = keyify(pk, dimension, period, metric)
        # Store pk dimensions
        dimension_key = keyify(pk, 'dimensions')
        dimension_json = keyify(dimension)
        if not dimension_json in self._added_dimensions[dimension_key]:
            self.sadd(dimension_key, dimension_json)
            self._added_dimensions[dimension_key].append(dimension_json)
        # Store dimensional subdimensions
        if(dimension != '_'):
            subdimension_key = keyify(pk, 'subdimensions', parent(dimension))
            if not dimension_json in self._added_subdimensions[subdimension_key]:
                self.sadd(subdimension_key, dimension_json)
                self._added_subdimensions[subdimension_key].append(dimension_json)
        return self.hincrby(key, dt, int(count))

    def retrieve(self, pk, dimensions, metrics, period='all', dt=None):
        nested = defaultdict(dict)
        if period == 'all':
            dt = 'time'
        for dimension in map(maybe_dumps, iterate_dimensions(dimensions)):
            for metric in map(maybe_dumps, metrics):
                hash_key = keyify(pk, dimension, period, metric)
                value_dict = self.hgetall(hash_key)
                if period == 'all' and dt == 'time':
                    nested[dimension][metric] = float(value_dict.get('time', 0))
                else:
                    nested[dimension][metric] = dict([
                        (k, float(v)) for k, v in value_dict.items()])
        return dict(nested)


class Whale(object):
    whale_driver_class = WhaleRedisDriver
    whale_driver_settings = {}

    def curry_whale_instance_methods(self, attr='id'):
        if hasattr(self, attr):
            curry_instance_attribute(attr, 'plotpoints', self)
            curry_instance_attribute(attr, 'ratio_plotpoints', self)
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
        metrics = metrics or ['hits']
        if isinstance(metrics, basestring):
            metrics = [metrics]
        period = period or Period.default_size()
        sparse = cls.whale_driver().retrieve(pk, dimensions, metrics, period=period)
        nonsparse = defaultdict(dict)
        for dimensions, metrics in sparse.items():
            for metric, points in metrics.items():
                dts = Period(*period.split('x')).datetimes_strs()
                nonsparse[dimensions][metric] = []
                for dt in dts:
                    if flot_time:
                        dt = to_flot_time(Period.parse_dt_str(dt))
                    value = points[dt] if dt in points else 0
                    nonsparse[dimensions][metric].append([dt, float(value)])
                nonsparse[dimensions][metric] = points_type(nonsparse[dimensions][metric])
        if depth > 0:
            for sub in cls.get_subdimensions(pk, dimensions):
                nonsparse = dict(nonsparse.items() +
                    cls.plotpoints(pk, sub, metrics, depth=depth - 1, period=period,
                        flot_time=flot_time, points_type=points_type).items())
        return nonsparse

    @classmethod
    def ratio_plotpoints(cls, pk, numerator_metric, denomenator_metric='hits',
            dimensions=None, depth=0, period=None, flot_time=False, points_type=dict):
        top, bot = numerator_metric, denomenator_metric
        pps = cls.plotpoints(pk, dimensions, [top, bot], depth=depth, period=period,
            flot_time=flot_time, points_type=points_type)

        # The function that makes the ratios
        def ratio_func(tup):
            dim, mets = tup
            tgt_iter = points_type is dict and mets[bot].items() or mets[bot]

            # A function to get the numerator from either points_type=dict or points_type=list
            def get_top(dt):
                # Easy, just use the dict index
                if points_type is dict:
                    return mets[top][dt]
                # Complicated, use i-based index
                else:
                    idx = i = 0
                    for dtb, valb in mets[bot]:
                        if dt == dtb:
                            idx = i
                        i += 1
                    return mets[top][idx][1]
            return (dim, points_type([(dt,
                    denom and (get_top(dt) / denom) or 0)
                                    for (dt, denom) in tgt_iter]))
        return dict(map(ratio_func, pps.items()))

    @classmethod
    def totals(cls, pk, dimensions=None, metrics=None, periods=None):
        if not periods:
            periods = DEFAULT_PERIODS
        if not isinstance(periods, list):
            periods = [periods]
        metrics = metrics or ['hits']
        if not isinstance(metrics, list):
            metrics = [metrics]
        d = {}
        for p in periods:
            p_data = cls.plotpoints(pk, dimensions, metrics, period=str(p))
            p_totals = dict()
            for dim, mets in p_data.items():
                p_totals[dim] = dict()
                for met, vals in mets.items():
                    p_totals[dim][met] = sum([
                        v for k, v in vals.items()
                        if Period.get(p).flatten(k)])
            d[str(p)] = p_totals
        d['alltime'] = cls.whale_driver().retrieve(
                pk, dimensions, metrics, period='all')
        return d

    @classmethod
    def reset(cls, pk, dimensions=None, metrics=None):
        r = cls.whale_driver().reset(
                pk, dimensions, metrics)
        return r

    @classmethod
    def cleanup(cls):
        ps = dict([(str(p), p) for p in DEFAULT_PERIODS])
        r = cls.whale_driver()
        keys = r.keys('*||*||*||*')
        for k in keys:
            try:
                val = r.hgetall(k)
            except:
                r.delete(k)
                continue
            this_p = k.split('||')[2]
            if this_p == 'all':
                continue
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
                print 'Key empty, deleting --', k
            elif deleted > 0:
                print 'Deleted', deleted, 'old keys from', k

    @classmethod
    def get_subdimensions(cls, pk, dimension='_'):
        if dimension == ['_']:
            dimension = '_'
        subdimensions = map(lambda s: map(str, try_loads(s)),
                cls.whale_driver().smembers(keyify(pk, 'subdimensions',
                    dimension)))
        return subdimensions

    @classmethod
    def all_subdimensions(cls, pk, dimension='_'):
        subdimensions = [dimension]
        for d in cls.get_subdimensions(pk, dimension):
            subdimensions += cls.all_subdimensions(pk, d)
<<<<<<< Updated upstream
<<<<<<< Updated upstream
=======
=======
>>>>>>> Stashed changes
        if dimension:
            subdimensions.append(dimension)
>>>>>>> Stashed changes
        return subdimensions

    @classmethod
    def count_now(cls, pk, dimensions, metrics=None, at=False):
        """ Immediately count a hit, as opposed to logging it into Hail"""
        periods = DEFAULT_PERIODS

        if isinstance(at, basestring):
            try:
                if ':' in at:
                    at = datetime.strptime(at, '%c')
                else:
                    at = float(at)
            except Exception as e:
                print e
        if not metrics:
            metrics = list()
        if type(metrics) == list:
            metrics = dict([(k, 1) for k in metrics])
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
            iterate_dimensions(dimensions) + ['_'],
                        generate_increments(metrics, periods, at)):
            cls.whale_driver().store(pk, dimension, metric, period, dt, i)

    @classmethod
    def rank_subdimensions_scalar(cls, pk, dimension='_', metric='hits', period=None):
        period = period or Period.default_size()
        d_k = keyify(dimension)
        total = cls.totals(pk, dimension, metric, periods=[period])[period][d_k][metric]
        ranked = dict()

        def info(sub):
            pps = cls.plotpoints(pk, sub, metric, period=period)[sub][metric]
            sub_total = sum(pps.values())
            return {
                'points': pps,
                'total': sub_total,
                'important': sub_total > 10 and (sub_total > (total / 10)) or False
            }

        for sub in map(maybe_dumps, cls.all_subdimensions(pk, dimension)):
            ranked[sub] = info(sub)
        del(ranked[dimension])

        # Prune parents
        for sub, info in ranked.items():
            children = map(maybe_dumps, cls.get_subdimensions(pk, sub))
            children_total = sum(map(lambda s: ranked[s]['total'], children))
            if info['important'] and (info['total'] - children_total) < (total / 10):
                info['important'] = False
        return ranked

    @classmethod
    def rank_subdimensions_ratio(cls, pk, dimension='_', numerator, denominator='hits'):
        top, bottom = numerator, denominator
        period = period or Period.default_size()
        d_k = keyify(dimension)
        top_total = cls.totals(pk, dimension, top, periods=[period])[period][d_k][top]
        bottom_total = cls.totals(pk, dimension, bottom, periods=[period])[period][d_k][bottom]
        ratio_total = bottom_total and float(top_total / bottom_total) or 0
        ranked = dict() 

        def info(sub):
            pps = cls.plotpoints(pk, sub, [top, bottom], period=period)[sub]
            ratio_points = cls.ratio_plotpoints(pk, sub, top, bottom, period=period)[sub]
            top_pps = pps[top]
            bottom_pps = pps[bottom]

            sub_top_sum = sum(top_pps.values())
            sub_bottom_sum = sum(bottom_pps.values())

            ratio = sub_bottom_sum and float(sub_top_sum / sub_bottom_sum) or 0

            difference = (ratio - ratio_total) / ratio_total

            important = sub_bottom_sum > 5 and (difference > .1 or -difference > .1)

            return {
                'points': pps
                'ratio_points': ratio_points
                'difference': difference
                'effect': difference * bottom
                'important': important
            }
        
        for sub in map(maybe_dumps, cls.all_subdimensions(pk, dimension)):
            ranked[sub] = info(sub)
        del(ranked[dimension])

        return ranked  



def iterate_dimensions(dimensions):
    if not dimensions:
        dimensions = []
    if isinstance(dimensions, dict):
        dimensions = list(nested_dict_to_list_of_keys(dimensions))
    elif isinstance(dimensions, basestring):
        dimensions = [dimensions, ]
    elif isinstance(dimensions, list) and len(dimensions) and not isinstance(dimensions[0], list):
        dimensions = [dimensions[:n + 1] for n in range(len(dimensions))]
    return dimensions


def generate_increments(metrics, periods=False, at=False):
    periods = periods or DEFAULT_PERIODS
    observations = set()
    at = at or datetime.now()
    for period in periods:
        dt = period.flatten_str(at)
        if not dt:
            continue
        observations.add((period, dt))
    rr = [(str(period), dt, metric, incr_by)
            for (period, dt) in observations
            for metric, incr_by in metrics.items()]
    for metric, incr_by in metrics.items():
        rr.append(('all', 'time', metric, incr_by))
    return rr
