from redis import Redis
from periods import DEFAULT_PERIODS, Period
from collections import defaultdict
from datetime import datetime
import json, itertools, collections
from util import to_flot_time, curry_instance_attribute
DELIM = '||'
def keyify(*args):
    json_args = [json.dumps(arg) if not isinstance(arg, basestring) else arg
            for arg in args]
    return DELIM.join([arg if arg not in 
        [None, False, '[null]', [], ['_'], '', '""', '"_"', '\"\"']
        else '_' for arg in json_args ])

class WhaleRedisDriver(Redis):
    def __init__(self, *args, **kwargs):
        super(WhaleRedisDriver, self).__init__(*args, **kwargs)
        self._added_dimensions = collections.defaultdict(list)
        self._added_subdimensions = collections.defaultdict(list)
    def store(self, pk, dimension, metric, period, dt, count):
        # Keep a list of graphs per category
        pk = json.dumps(pk)
        key = keyify(pk, json.dumps(dimension), period, metric)
        # Store category dimensions
        dimension_key = keyify(pk,'dimensions')
        dimension_json = isinstance(dimension, basestring) and dimension or json.dumps(dimension)
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
    def retrieve(self, pk, dimensions, metrics, period='all', dt=None,
            overall=True):
        pk = json.dumps(pk)
        nested = defaultdict(dict)
        to_i = lambda n: int(n) if n else 0
        if period=='all': dt='time'
        for dimension in map(json.dumps, iterate_dimensions(dimensions)):
            for metric in metrics:
                if not isinstance(dimension, basestring):
                    dimension = json.dumps(dimension) 
                elif dimension == '"_"':
                    dimension = '_'
                if dimension == '_' and overall == False: continue
                hash_key = keyify(pk, dimension, period, metric)
                value_dict = self.hgetall(hash_key)
                if period=='all' and dt == 'time':
                    nested[dimension][metric] = float(value_dict.get('time', 0))
                else: nested[dimension][metric] = dict([
                        (k, float(v)) for k,v in value_dict.items()])
        return dict(nested)
    def get_subdimensions(self, category, dimension=None):
        if not dimension: return '_'
        return self.smembers(keyify(category,'subdimensions', dimension))
    def all_subdimensions(self, category, dimension=None):
        subdimensions = []
        for d in self.get_subdimensions(category, dimension):
            subdimensions += self.all_subdimensions(category, d)
        if dimension: 
            if isinstance(dimension, list) and len(dimension) == 1:
                subdimensions.append(dimension[0])
            else:
                subdimensions.append(dimension)
        return subdimensions

class Whale():
    whale_driver_class = WhaleRedisDriver
    whale_driver_settings = {}
    def __init__(self, *args, **kwargs):
        if hasattr(self, 'id'):
            curry_instance_attribute('id', 'plotpoints', self)
            curry_instance_attribute('id', 'totals', self)
            curry_instance_attribute('id', 'count_now', self)
            curry_instance_attribute('id', 'reset', self)

    def whale_driver(self):
        if not hasattr(self, '_whale_driver'):
            self._whale_driver = self.whale_driver_class(**self.whale_driver_settings)
        return self._whale_driver
    def generate_increments(self, metrics, periods=False, at=False):
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

    def plotpoints(self, pk=None, dimensions=None, metrics=None,
            period=None, overall=True):
        pk = pk or []
        metrics = metrics or ['hits',]
        period = period or Period.default_size()
        sparse = self.whale_driver().retrieve(pk,dimensions,metrics,
                period=period, overall=overall)
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

    def totals(self, pk=None, dimensions=None, metrics=None):
        metrics = metrics or ['hits',]
        d = {}
        for p in DEFAULT_PERIODS: 
            p_data = self.whale_driver().retrieve(
                pk,dimensions,metrics,period=str(p))
            p_totals = dict()
            for dim, mets in p_data.items():
                p_totals[dim] = dict()
                for met, vals in mets.items():
                    p_totals[dim][met] = sum([
                        v for k,v in vals.items()
                        if p.flatten(k)])
            d[str(p)] = p_totals
        d['alltime'] = self.whale_driver().retrieve(
                pk, dimensions, metrics, period='all')
        return d

    def reset(self, pk=None, dimensions=None, metrics=None):
        r= self.whale_driver().reset(
                pk,dimensions,metrics)
        return r
    def cleanup(self):
        from periods import DEFAULT_PERIODS
        ps = dict([(str(p), p) for p in DEFAULT_PERIODS])
        r = self.whale_driver()
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

    def count_now(self, pk, dimensions, metrics, at=False):
        """ Immediately count a hit, as opposed to logging it into Hail"""
        import time, random
        r=self.whale_driver()
        periods = DEFAULT_PERIODS

        if isinstance(at, basestring):
            try:
                if ':' in at: at = datetime.strptime(at, '%c')
                else: at = float(at)
            except Exception as e: print e
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
                        self.generate_increments(metrics, periods, at)):
            self._whale_driver.store(pk, dimension, metric, period, dt, i)

def iterate_dimensions(dimensions):
    from util import nested_dict_to_list_of_keys
    if not dimensions: dimensions = '_' 
    if isinstance(dimensions, dict):
        dimensions = list(nested_dict_to_list_of_keys(dimensions))
    elif type(dimensions) in [str, unicode]:
        dimensions = [dimensions, ]
    elif isinstance(dimensions, list) and len(dimensions) and not isinstance(dimensions[0], list):
        dimensions = [dimensions, ]
    return dimensions

