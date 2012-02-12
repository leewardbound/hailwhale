import json
import math
import itertools
import collections
import times

from redis import Redis
from collections import defaultdict
from datetime import datetime

from util import to_flot_time, curry_instance_attribute
from util import nested_dict_to_list_of_keys, whale_cache
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
        key = keyify(pk, dimension, str(period), metric)
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

    def retrieve(self, pk, dimensions, metrics, period=None, dt=None):
        nested = defaultdict(dict)
        period = str(Period.get(period))
        for dimension in map(maybe_dumps, iterate_dimensions(dimensions)):
            for metric in map(maybe_dumps, metrics):
                hash_key = keyify(pk, dimension, period, metric)
                value_dict = self.hgetall(hash_key)
                nested[dimension][metric] = dict([
                        (k, float(v)) for k, v in value_dict.items()])
        return dict(nested)


class Whale(object):
    whale_driver_class = WhaleRedisDriver
    whale_driver_settings = {}

    def curry_whale_instance_methods(self, attr='id'):
        if hasattr(self, attr):
            for method in ['plotpoints', 'ratio_plotpoints', 'scalar_plotpoints',
                'totals', 'count_now', 'count_decided_now', 'decide',
                'weighted_reasons', 'reasons_for']:
                curry_instance_attribute(attr, method, self,
                        with_class_name=True)

    @classmethod
    def whale_driver(cls):
        if not hasattr(cls, '_whale_driver'):
            cls._whale_driver = cls.whale_driver_class(**cls.whale_driver_settings)
        return cls._whale_driver

    @classmethod
    def scalar_plotpoints(cls, pk, dimensions=None, metrics=None,
            depth=0, period=None, flot_time=False, points_type=dict):
        metrics = metrics or ['hits']
        if isinstance(metrics, basestring):
            metrics = [metrics]
        period = Period.get(period)
        sparse = cls.whale_driver().retrieve(pk, dimensions, metrics, period=period)
        nonsparse = defaultdict(dict)
        if flot_time:
            points_type = list
        for dim, mets in sparse.items():
            for met, points in mets.items():
                dts = period.datetimes_strs()
                nonsparse[dim][met] = []
                for dt in dts:
                    if flot_time:
                        dt_t = to_flot_time(Period.parse_dt_str(dt))
                    else:
                        dt_t = dt
                    value = points[dt] if dt in points else 0
                    nonsparse[dim][met].append([dt_t, float(value)])
                nonsparse[dim][met] = points_type(nonsparse[dim][met])
        if depth > 0:
            for sub in cls.get_subdimensions(pk, dimensions):
                nonsparse = dict(nonsparse.items() +
                    cls.plotpoints(pk, sub, metrics, depth=depth - 1, period=period,
                        flot_time=flot_time, points_type=points_type).items())
        return nonsparse

    @classmethod
    def plotpoints(cls, pk, dimensions=None, metrics=None, **kwargs):
        """ Combines scalar_plotpoints and ratio_plotpoints into a single func call w/ formula support """
        combo = defaultdict(dict)
        scalars = []
        ratios = {}
        metrics = metrics or ['hits']
        metrics = isinstance(metrics, list) and metrics or [metrics,]
        sort = kwargs.pop('sort', None)
        if not sort:
            sort = '-'+metrics[0]
        limit = kwargs.pop('limit', 0)
        reverse = False
        if sort[0] == '-': 
            sort = sort[1:]
            reverse = True
        if not sort in metrics:
            metrics.append(sort)

        # Figure out which ones are ratios and pre-fetch
        if isinstance(metrics, basestring):
            metrics = [metrics]
        for met in metrics:
            top, bottom = parse_formula(met)
            if not bottom:
                scalars.append(met)
            else:
                ratios[met] = cls.ratio_plotpoints(pk, top, bottom, dimensions, **kwargs)

        # Now get the scalars
        combo = cls.scalar_plotpoints(pk, dimensions, scalars, **kwargs)
        # and merge them
        for dim, mets in combo.items():
            for ratio, points in ratios.items():
                combo[dim][ratio] = points[dim][ratio]

        # Begin Sorting and trimming fun

        # Get the values from either a dict or a list
        def _vals(pts):
            if isinstance(pts, list):
                return map(lambda (x,y): y, pts)
            elif isinstance(pts, dict):
                return pts.values()
            return []
        # Get the total from the points
        _tot = lambda dim: sum(_vals(combo[dim][sort]))
        # Now tally the scores
        scores = sorted([(dim, _tot(dim)) for dim in combo.keys()],
            key=lambda tup: tup[1], reverse=reverse)
        if limit:
            scores = scores[:limit]
        high_scores = dict(scores)

        return dict([(d,m) for d,m in combo.items()
            if d in high_scores
            ])

    @classmethod
    @whale_cache
    def cached_plotpoints(cls, *args, **kwargs):
        return cls.plotpoints(*args, **kwargs)

    @classmethod
    def ratio_plotpoints(cls, pk, numerator_metric, denomenator_metric='hits',
            dimensions=None, depth=0, period=None, flot_time=False, points_type=dict):
        top, bot = numerator_metric, denomenator_metric
        pps = cls.scalar_plotpoints(pk, dimensions, [top, bot], depth=depth, period=period,
            flot_time=flot_time, points_type=points_type)
        formula = '%s/%s' % (top, bot)

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
            return (dim, {formula:
                    points_type([(dt, (denom and (get_top(dt) / denom) or 0))
                                    for (dt, denom) in tgt_iter])})
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
        ratios = []
        for metric in metrics:
            if '/' in metric:
                metrics.remove(metric)
                ratios.append(metric)
                metrics += metric.split('/')
        d = {}
        for p in periods:
            p_data = cls.plotpoints(pk, dimensions, metrics, period=str(p))
            p_totals = dict()
            for dim in p_data.keys():
                p_totals[dim] = dict()
                for met, vals in p_data[dim].items():
                    p_totals[dim][met] = sum([
                        v for k, v in vals.items()
                        if Period.get(p).flatten(k)])
                for rat in ratios:
                    top, bot = parse_formula(rat)
                    topt, bott = p_totals[dim][top], p_totals[dim][bot]
                    p_totals[dim][rat] = bott and topt / bott or 0
            d[str(p)] = p_totals
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
        if dimension != '_' and not isinstance(dimension, list):
            dimension = [dimension]
        set_members = cls.whale_driver().smembers(keyify(pk, 'subdimensions',
                    dimension))
        subdimensions = []
        for s in set_members:
            loaded = try_loads(s)
            if isinstance(loaded, list) and len(loaded):
                loaded = map(str, loaded)
            subdimensions.append(loaded)
        return subdimensions

    @classmethod
    def all_subdimensions(cls, pk, dimension='_'):
        subdimensions = [dimension]
        for d in cls.get_subdimensions(pk, dimension):
            subdimensions += cls.all_subdimensions(pk, d)
        return map(try_loads, subdimensions)

    @classmethod
    def count_decided_now(cls, pk_base, decision, option, *args, **kwargs):
        return cls.count_now([pk_base, decision, option], *args, **kwargs)

    @classmethod
    def count_now(cls, pk, dimensions='_', metrics=None, at=False):
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
        for pkk, dimension, (period, dt, metric, i) in itertools.product(
            iterate_dimensions(pk),
            iterate_dimensions(dimensions, add_root=True),
                        generate_increments(metrics, periods, at)):
            cls.whale_driver().store(pkk, dimension, metric, period, dt, i)

    @classmethod
    def rank_subdimensions_scalar(cls, pk, dimension='_', metric='hits',
            period=None, recursive=True, prune_parents=True, points=False):
        period = period or Period.default_size()
        d_k = keyify(dimension)
        total = cls.totals(pk, dimension, metric, periods=[period])[period][d_k][metric]
        ranked = dict()

        def info(sub):
            pps = cls.plotpoints(pk, sub, metric, period=period)[sub][metric]
            sub_total = sum(pps.values())
            data = {
                'points': pps,
                'score': sub_total,
                'important': sub_total > 10 and (sub_total > (total / 10)) or False,
                'effect': total - sub_total,
                'difference': total - sub_total,
                'value': sub_total,
                'count': sub_total,
                'dimension': sub
            }
            if not points:
                del data['points']
            return data
        _subs = recursive and cls.all_subdimensions or cls.get_subdimensions
        for sub in map(maybe_dumps, _subs(pk, dimension)):
            ranked[sub] = info(sub)

        # Prune parents
        if recursive and prune_parents:
            for sub, info in ranked.items():
                children = map(maybe_dumps, cls.get_subdimensions(pk, sub))
                children_total = sum(map(lambda s: ranked[s]['score'], children))
                if info['important'] and (info['score'] - children_total) < (total / 10):
                    info['important'] = False
        return ranked

    @classmethod
    def rank_subdimensions_ratio(cls, pk, numerator, denominator='hits',
            dimension='_', period=None, recursive=True, points=False):
        top, bottom = numerator, denominator
        period = period or Period.default_size()
        d_k = keyify(dimension)
        top_total = cls.totals(pk, dimension, top, periods=[period])[str(period)][d_k][top]
        bottom_total = cls.totals(pk, dimension, bottom, periods=[period])[str(period)][d_k][bottom]
        ratio_total = bottom_total and float(top_total / bottom_total) or 0
        ranked = dict()

        def info(sub):
            pps = cls.plotpoints(pk, sub, [top, bottom, '%s/%s' % (top, bottom)], period=period)[sub]
            top_pps = pps[top]
            bottom_pps = pps[bottom]

            sub_top_sum = sum(top_pps.values())
            sub_bottom_sum = sum(bottom_pps.values())

            ratio = sub_bottom_sum and float(sub_top_sum / sub_bottom_sum) or 0

            difference = ratio_total and (ratio - ratio_total) / ratio_total or 0
            important = sub_bottom_sum > 5 and math.fabs(difference) > .1

            data = {
                'points': pps,
                'score': ratio,
                'difference': difference,
                'effect': difference * sub_bottom_sum * ratio_total,
                'value': sub_top_sum,
                'count': sub_bottom_sum,
                'important': important,
                'dimension': sub
            }
            if not points:
                del data['points']
            return data

        _subs = recursive and cls.all_subdimensions or cls.get_subdimensions

        for sub in map(maybe_dumps, _subs(pk, dimension)):
            ranked[sub] = info(sub)

        return ranked

    @classmethod
    def rank(cls, pk, formula, dimension='_', period=None, recursive=True,
            prune_parents=True, points=False):
        top, bot = parse_formula(formula)
        if not bot:
            return cls.rank_subdimensions_scalar(pk, top, dimension, period, recursive, prune_parents)
        else:
            return cls.rank_subdimensions_ratio(pk, top, bot, dimension, period, recursive)

    @classmethod
    @whale_cache
    def cached_rank(cls, *args, **kwargs):
        return cls.rank(*args, **kwargs)

    @classmethod
    def decide_from_reasons(cls, good, bad, test, bad_idea_threshold=.05,
        test_idea_threshold=.05):
        import random

        def w_choice(reasons):
            weights = map(lambda k: k.get('weight', 1), reasons.values())
            n = random.uniform(0, sum(weights))
            for item, opts in reasons.items():
                if n < opts.get('weight', 1):
                    break
                n = n - opts.get('weight', 1)
            return item
        # if there are tests to be ran, and either
        # - there are no good or bad choices, or
        # - the test threshold is triggered randomly
        if len(test) and ((not len(good) and not len(bad)) or (
                random.random() < test_idea_threshold)):
            return w_choice(test)
        # if there are bad choices and we have hit the badness roulette,
        # pick a weighted bad idea
        if not len(good) or random.random() < bad_idea_threshold and len(bad):
            return w_choice(bad)
        else:
            return w_choice(good)

    @classmethod
    def decide(cls, pk_base, decision_name, options, formula='value/hits', known_data=None,
        period=None, bad_idea_threshold=.05, test_idea_threshold=.05):
        good, bad, test = cls.weighted_reasons(pk_base, decision_name, options,
                formula=formula, known_data=known_data, period=period)
        return cls.decide_from_reasons(good, bad, test, bad_idea_threshold=bad_idea_threshold,
            test_idea_threshold=test_idea_threshold)

    @classmethod
    def weighted_reasons(cls, pk_base, decision_name, options, formula='value/hits',
        known_data=None, period=None, recursive=True):
        good, bad, test = defaultdict(dict), defaultdict(dict), defaultdict(dict)
        for o in options:
            opk = [pk_base, decision_name, o]
            i = cls.reasons_for(opk, formula=formula, known_data=known_data,
                    period=period)
            best = i['good'] or i['base']
            worst = i['bad'] and i['bad'] or i['base']
            if best['effect'] > 1 and best['significance'] > 2:
                good[o] = i
            elif worst['effect'] < -1 or (worst['effect'] < 0 and worst['significance'] > 2):
                bad[o] = i
            else:
                test[o] = i
        total_goodness = sum(map(lambda g: math.fabs(g['good'].get('effect', 1)), good.values())) or 1
        total_badness = sum(map(lambda b: math.fabs(b['bad'].get('effect', 1)), bad.values())) or 1
        for opt, g in good.items():
            good[opt]['weight'] = math.fabs(g['good'].get('effect', 1) / total_goodness)
        for opt, b in bad.items():
            bad[opt]['weight'] = math.fabs(b['bad'].get('effect', 1) / total_badness)
        for opt, t in test.items():
            test[opt]['weight'] = 1
        return good, bad, test

    @classmethod
    def reasons_for(cls, pk, formula='value/hits', known_data=None, period=None, recursive=True):
        metric, denomenator = parse_formula(formula)
        period = Period.get(period)
        pk_base, decision, option = pk
        base = '_'
        best = worst = None
        ranks = cls.cached_rank(pk, formula=formula, dimension=base,
            period=period, recursive=recursive, points=False)
        overall = cls.cached_rank([pk_base, decision], formula=formula, dimension=base,
            period=period, recursive=recursive, points=False)
        parent_score = overall[base]['score']
        parent_count = overall[base]['count']
        ranks[base]['effect'] = ranks[base]['count'] * ranks[base]['difference']

        def delta(info):
            diff = info['score'] - parent_score
            info['value_diff'] = info['value'] - overall[base]['value']
            info['difference'] += diff
            if math.fabs(diff) > 0  and info['count'] > 0:
                info['effect'] += diff * info['count']
                info['significance'] = ((.5 * info['effect']) ** 2) / parent_count
            else:
                info['effect'] = 0
                info['significance'] = 0
            return info
        known_dimensions = iterate_dimensions(known_data)
        for dim, info in ranks.items():
            ranks[dim] = info = delta(info)
            if try_loads(dim) in known_dimensions and info['important']:
                best_score = best and ranks[best]['score']
                worst_score = worst and ranks[worst]['score']
                if info['score'] > best_score:
                    best = dim
                if info['score'] < worst_score:
                    worst = dim
        i = {'good': best and ranks[best] or {},
                'bad': worst and ranks[worst] or {},
                #'ranks': ranks,
                'base': ranks[base],
                'parent': overall[base]}
        i['high'] = i['good'].get('difference', 0)
        i['high_sig'] = i['good'].get('significance', 0) > 4
        i['low'] = i['bad'].get('difference', 0)
        i['low_sig'] = i['bad'].get('significance', 0) > 4
        return i

def parse_formula(formula):
    if not '/' in formula:
        return (formula, None)
    else:
        return formula.split('/')

def iterate_dimensions(dimensions, add_root=False):
    if not dimensions:
        dimensions = []
    if isinstance(dimensions, dict):
        dimensions = list(nested_dict_to_list_of_keys(dimensions))
    elif isinstance(dimensions, basestring):
        dimensions = [dimensions, ]
    elif isinstance(dimensions, list) and len(dimensions) and not isinstance(dimensions[0], list):
        dimensions = [dimensions[:n + 1] for n in range(len(dimensions))]
    if add_root and not '_' in dimensions and not ['_'] in dimensions:
        dimensions.append('_')
    return dimensions


def generate_increments(metrics, periods=False, at=False):
    periods = periods or DEFAULT_PERIODS
    observations = set()
    at = at or times.now()
    for period in periods:
        dt = period.flatten_str(at)
        if not dt:
            continue
        observations.add((period, dt))
    rr = [(str(period), dt, metric, incr_by)
            for (period, dt) in observations
            for metric, incr_by in metrics.items()]
    return rr
