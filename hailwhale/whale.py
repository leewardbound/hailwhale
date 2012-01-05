from redis import Redis
from periods import DEFAULT_PERIODS, Period
from collections import defaultdict
from datetime import datetime
import json, itertools
from util import to_flot_time
DELIM = '||'
def keyify(*args):
    json_args = [json.dumps(arg) if not isinstance(arg, basestring) else arg
            for arg in args]
    return DELIM.join([arg if arg not in 
        [None, False, '[null]', [], ['_'], '', '""', '"_"', '\"\"']
        else '["_"]' for arg in json_args ])
class WhaleRedisDriver(Redis):
    def store(self, categories, dimension, metric, period, dt, count):
        if type(categories) in [str,unicode]: categories = [categories,]
        # Keep a list of graphs per category
        cats_str = json.dumps(categories)
        key = keyify(cats_str, json.dumps(dimension), period, metric)
        # Store category dimensions
        self.sadd(keyify(categories,'dimensions',cats_str),
                json.dumps(dimension))
        # Store dimensional subdimensions
        if len(dimension) > 1:
            parent_dimension = dimension[:-1]
        else: parent_dimension = '["_"]'
        if(dimension != '["_"]'):
            self.sadd(keyify(categories,'subdimensions',cats_str,parent_dimension),
                json.dumps(dimension))
        return self.hincrby(key, dt, int(count))
    def retrieve(self, categories, dimensions, metrics, period='all', dt=None,
            depth=0,overall=True):
        if type(categories) in [str,unicode]: categories = [categories,]
        cats_str = json.dumps(categories)
        if depth > 0: 
            extra_dimensions = list() 
            for d in self.smembers(keyify(categories, 'subdimensions',
                        cats_str, dimensions)):
                try: extra_dimensions += [json.loads(d)]
                except Exception as e:print e
            dimensions = extra_dimensions + [dimensions[0]]
        nested = defaultdict(dict)
        to_i = lambda n: int(n) if n else 0
        if period=='all': dt='time'
        conversions = {}
        if isinstance(metrics, dict):
            conversions = metrics
            metrics = conversions.keys()
        for dimension in map(json.dumps, iterate_dimensions(dimensions)):
            for metric in metrics:
                if not isinstance(dimension, basestring):
                    dimension = json.dumps(dimension) 
                elif dimension == '"_"':
                    dimension = '["_"]'
                if dimension == '["_"]' and overall == False: continue
                key = keyify(cats_str, dimension, period, metric)
                value_dict = self.hgetall(key)
                if metric in conversions and conversions[metric] not in [1,'1']:
                    if conversions[metric] == 'avg': second_key = 'hits'
                    else: second_key = conversions[metric]
                    key = keyify(cats_str, dimension, period, second_key)
                    avg_dict = self.hgetall(key)
                    for flottime, val in value_dict.items():
                        try: value_dict[flottime] = float(val) / float(avg_dict[flottime])
                        except Exception as e: 
                            print e
                            value_dict[flottime] = 0
                if dt == 'time': nested[d_p][metric] = value_dict['time']
                else: nested[dimension][metric] = value_dict
        return dict(nested)

class Whale():
    driver_class = WhaleRedisDriver
    driver_settings = {}
    def whale_driver(self):
        if not hasattr(self, '_whale_driver'):
            self._whale_driver = self.driver_class(**self.driver_settings)
        return self._whale_driver
    def dotproduct_keys(self, metrics, periods=False, at=False):
        from itertools import product
        periods = periods or DEFAULT_PERIODS
        observations = set()
        at = at or datetime.utcnow()
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

    def plotpoints(self, categories=None, dimensions=None, metrics=None,
            period=None, depth=0, overall=True):
        categories = categories or ''
        dimensions = dimensions or json.dumps(list(list()))
        # Convert categories to a list, if it's not
        if type(categories) in [str,unicode]: categories = [categories,]
        metrics = metrics or ['hits',]
        period = period or Period.default_size()
        sparse = self.whale_driver().retrieve(categories,dimensions,metrics,
                period=period, depth=depth, overall=overall)
        nonsparse = defaultdict(dict)
        for dimensions, metrics in sparse.items():
            for metric, points in metrics.items():
                dts = Period(*period.split('x')).datetimes_strs()
                nonsparse[dimensions][metric] = []
                for dt in dts:
                    flot_time = to_flot_time(Period.parse_dt_str(dt))
                    value = points[dt] if dt in points else 0
                    nonsparse[dimensions][metric].append([flot_time,
                        float(value)])
        return nonsparse


    def totals(self, categories=None, dimensions=None, metrics=None):
        categories = categories or ''
        dimensions = dimensions or json.dumps(list(list()))
        metrics = metrics or ['hits',]
        return self.whale_driver().retrieve(categories,dimensions,metrics)

    def count_now(self, categories, dimensions, metrics, at=False):
        """ Immediately count a hit, as opposed to logging it into Hail"""
        import time, random
        r=self.whale_driver()
        periods = DEFAULT_PERIODS

        # Convert categories to a list, if it's not
        if type(categories) == str: categories = [categories,]
        metrics['hits'] = 1
        # Dimensions: {a: 5, b: {x: 1, y: 2}} --> will increment each of: 
        # [a],
        # [a, 5], 
        # [b], 
        # [b, x], 
        # [b, x, 1],
        # [b, y], 
        # [b, y, 2]
        for dimension, (period, dt, metric, i) in itertools.product(
            iterate_dimensions(dimensions),
                        self.dotproduct_keys(metrics, periods, at)):
            self._whale_driver.store(categories, dimension, metric, period, dt, i)

def iterate_dimensions(dimensions):
    from util import nested_dict_to_list_of_keys
    if not dimensions: dimensions = ['empty',]
    if type(dimensions) is dict:
        dimensions = list(nested_dict_to_list_of_keys(dimensions))
    elif type(dimensions) in [str, unicode]:
        dimensions = [dimensions, ]
    elif type(dimensions) is list and type(dimensions[0]) is not list:
        dimensions = [dimensions, ]
    if not ['_'] in dimensions: dimensions += [ ['_',] ]
    return dimensions
