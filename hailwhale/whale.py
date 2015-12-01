import json
import math
import itertools
import collections
import urllib

from redis import Redis
from collections import defaultdict, OrderedDict
from datetime import datetime, timedelta

from util import *
from periods import DEFAULT_PERIODS, Period, MAX_INTERVALS

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


_added_dimensions = collections.defaultdict(list)
_added_subdimensions = collections.defaultdict(list)


def _increment(*args, **kwargs):
    kwargs['method'] = 'incr'
    return _store(*args, **kwargs)

def _store(redis, pk, dimension, metric, interval, dt, count, method='set',
        rank=False):
    # Keep a list of graphs per pk
    key = keyify(pk, dimension, interval, metric)
    # Store pk dimensions
    dimension_key = keyify('dimensions', pk)
    dimension_json = keyify(dimension)
    if not dimension_json in _added_dimensions[dimension_key]:
        redis.sadd(dimension_key, dimension_json)
        _added_dimensions[dimension_key].append(dimension_json)
    # Store dimensional subdimensions
    if dimension != '_':
        subdimension_key = keyify('subdimensions', pk, parent(dimension))
        if not dimension_json in _added_subdimensions[subdimension_key]:
            redis.sadd(subdimension_key, dimension_json)
            _added_subdimensions[subdimension_key].append(dimension_json)

    if method == 'set':
        new_val = int(count)
        redis.hset(key, dt, new_val)
    elif method == 'incr':
        new_val = redis.execute_command('HINCRBY', key, dt, int(count))
    if rank and (isinstance(try_loads(pk), list) or dimension != '_'):
        if isinstance(pk, list) and dimension == '_':
            tgt_pk = parent(pk)
            tgt_dimension = dimension
        else:
            tgt_pk = pk
            tgt_dimension = parent(dimension)
        rank_key = keyify('rank', tgt_pk, tgt_dimension, interval, dt, metric)
        redis.zadd(rank_key, dimension_json, new_val)
    return new_val

def _ranked(redis, pk, parent_dimension, metric, period, ats, start=0, size=10,
        sort_dir=None):
    top, bot = parse_formula(metric)
    rank_keyify = lambda ats, met: keyify('rank', pk, parent_dimension,
            Period.get(period).interval, ats, met)
    final_rank_key = rank_keyify(ats, metric)
    def squash_ats(met):
        if len(ats) > 1:
            map(lambda at: redis.zremrangebyscore(rank_keyify(at, met), 0, 0), ats)
            redis.zunionstore(rank_keyify(ats, met),
                    map(lambda at: rank_keyify(at, met), ats))
    squash_ats(top)
    if bot:
        squash_ats(bot)
        top_key, bot_key = rank_keyify(ats, top), rank_keyify(ats, bot)
        redis.execute_command("eval", """
        for key_i, key_n in ipairs(redis.call("zrange", KEYS[2], 0, -1)) do
            local top_s = tonumber(redis.call("zscore", KEYS[1], key_n))
            local bot_s = tonumber(redis.call("zscore", KEYS[2], key_n))
            if top_s and bot_s and bot_s > 0 then
                redis.call("zadd", KEYS[3], top_s/bot_s, key_n)
            end
        end
        """, 3, top_key, bot_key, final_rank_key)
        redis.zremrangebyscore(final_rank_key, 0, 0)
    return redis.zrange(final_rank_key, start, start + size,
                desc=not sort_dir or sort_dir.upper() in ['-', 'DESC', 'HIGH'])

def _retrieve(redis, pk, dimensions, metrics, period=None, dt=None):
    nested = defaultdict(dict)
    interval = Period.get(period).interval
    for dimension in iterate_dimensions(dimensions)+['_']:
        for metric in metrics:
            if ':' in metric:
                metric_name = metric.split(':')[0]
            else: metric_name = metric
            hash_key = keyify(pk, dimension, interval, metric_name)
            value_dict = redis.hgetall(hash_key)
            nested[maybe_dumps(dimension)][maybe_dumps(metric)] = dict([
                    (k, float(v)) for k, v in value_dict.items()])
    return dict(nested)


class Whale(object):
    whale_driver_settings = {}
    _local = None

    def curry_whale_instance_methods(self, attr='id'):
        if hasattr(self, attr) and not hasattr(self, '_hw_curried'):
            for method in ['plotpoints', 'ratio_plotpoints', 'scalar_plotpoints',
                'totals', 'count_now', 'count_decided_now', 'decide',
                'weighted_reasons', 'reasons_for', 'graph_tag', 'today', 'cached_rank', 'rank',
                'yesterday', 'update_count_to', 'total', 'render_hw_plotpoint_table', 'get_subdimensions',
                'all_subdimensions', 'rank_subdimensions_scalar', 'rank_subdimensions_ratio']:
                curry_instance_attribute(attr, method, self,
                        with_class_name=True)
            # Currying for related models as
            for method in ['plotpoints', 'graph_tag', 'count_now', 'totals']:
                curry_related_dimensions(attr, method, self, with_class_name=True)
            self._hw_curried = True


    @classmethod
    def graph_tag(cls, pk, dimension=None, metric=None, extra=None, host=''):
        if not extra:
            extra = {}
        extra['pk'] = maybe_dumps(pk)
        if dimension:
            extra['dimension'] = maybe_dumps(dimension)
        if metric:
            extra['metric'] = maybe_dumps(metric)
        if not 'title' in extra:
            extra['title'] = cls.__name__
        if not 'area' in extra:
            extra['area'] = 'true'
            extra['depth'] = 1
        return "<script src='%s/graph.js?%s'></script>" % (host, urllib.urlencode(extra.items()))

    @classmethod
    def class_graph_tag(cls, *args, **kwargs):
        return cls.graph_tag(cls.__name__, *args, **kwargs)

    @classmethod
    def whale_driver(cls):
        if not hasattr(cls, '_whale_driver'):
            cls._whale_driver = Redis(**cls.whale_driver_settings)
        return cls._whale_driver

    @classmethod
    def plotpoints(cls, pk, dimensions=None, metrics=None, **kwargs):
        """ Combines scalar_plotpoints and ratio_plotpoints into a single func call w/ formula support """
        scalars = []
        ratios = {}
        metrics = metrics or ['hits']
        only_metric = False
        tzoffset = kwargs.get('tzoffset', 0.0)
        if isinstance(metrics, basestring):
            metrics = [metrics]
        if isinstance(metrics, dict):
            metrics = ['%s%s' % (k, v != 1 and '/' + v or '') for k, v in metrics.items()]
        if len(metrics) == 1:
            only_metric = metrics[0]
        sort = kwargs.pop('sort', None)
        if not sort:
            if isinstance(metrics, list):
                sort = '-' + metrics[0]
            elif isinstance(metrics, dict):
                sort = '-' + metrics.keys()[0]
            else:
                sort = '-hits'
        limit = kwargs.pop('limit', 0)
        reverse = False
        if sort[0] == '-':
            sort = sort[1:]
            reverse = True

        # Figure out which ones are ratios and pre-fetch
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
        return dict([(d,
                dict([(m, ps) for m, ps in ms.items() if (m == only_metric or not only_metric)])
                ) for d,ms in combo.items()
            if  (d in high_scores or d in iterate_dimensions(dimensions))
            ])

    @classmethod
    @whale_cache
    def cached_plotpoints(cls, *args, **kwargs):
        return cls.plotpoints(*args, **kwargs)

    @classmethod
    def scalar_plotpoints(cls, pk, dimensions=None, metrics=None,
            depth=0, period=None, tzoffset=None, flot_time=False,
            points_type=OrderedDict, descending=False, with_totals=False):
        metrics = metrics or ['hits']
        if isinstance(metrics, basestring):
            metrics = [metrics]
        p_obj, ats, tzoffset = Period.get_days(period, tzoffset=tzoffset)
        p_s = str(p_obj)
        dts = list(p_obj.datetimes_strs(tzoffset=tzoffset))
        sparse = _retrieve(cls.whale_driver(), pk, dimensions, metrics, period=p_obj)
        nonsparse = defaultdict(dict)
        if flot_time:
            points_type = list
        for dim, mets in sparse.items():
            for met, points in mets.items():
                nonsparse[dim][met] = []
                use_method = False
                met_name = met
                if ':' in met:
                    met_name, use_method = met.split(':')

                const_value = False
                if met_name in TIME_MATRIX:
                    const_value = float(p_obj.getUnits()[0] /
                            TIME_MATRIX[met_name])
                # Try to parse static metrics too
                elif met_name == '_count':
                    const_value = len(dts)
                try:
                    const_value = float(met_name)
                except:
                    pass
                last_value = total = 0.0
                for dt in dts:
                    dt_obj = Period.parse_dt_str(dt)
                    if met_name == '_days_in_month':
                        from calendar import monthrange
                        const_value = monthrange(dt_obj.year, dt_obj.month)[1]
                    if flot_time:
                        dt_t = to_flot_time(dt_obj)
                    else:
                        dt_t = dt
                    if const_value:
                        value = const_value
                    else:
                        value = points[dt] if dt in points else 0
                    total += value
                    if use_method == 'count' or not use_method:
                        value = value
                    elif use_method in ['+', 'sum', 'add', 'cumulative']:
                        value = total
                    elif use_method in ['_', 'set', 'last', 'level']:
                        if not last_value:
                            last_value = value
                        if not value:
                            value = last_value
                        last_value = value
                    nonsparse[dim][met].append([dt_t, float(value)])
                if with_totals:
                    nonsparse[dim][met].append(['Total', total])
                if descending:
                    nonsparse[dim][met] = reversed(nonsparse[dim][met])
                nonsparse[dim][met] = points_type(nonsparse[dim][met])
        if depth > 0:
            for sub in cls.get_subdimensions(pk, dimensions):
                nonsparse = dict(nonsparse.items() +
                    cls.plotpoints(pk, sub, metrics, depth=depth - 1, period=period,
                        descending=descending, with_totals=with_totals,
                        flot_time=flot_time, points_type=points_type).items())
        return nonsparse

    @classmethod
    def ratio_plotpoints(cls, pk, numerator_metric, denomenator_metric='hits',
            dimensions=None, depth=0, period=None, flot_time=False, tzoffset=None,
            points_type=OrderedDict, descending=False, with_totals=False):
        if flot_time:
            points_type = list
        top, bot = numerator_metric, denomenator_metric
        pps = cls.scalar_plotpoints(pk, dimensions, [top, bot], depth=depth, period=period,
            flot_time=flot_time, points_type=points_type, tzoffset=tzoffset,
            descending=descending, with_totals=with_totals)
        formula = '%s/%s' % (top, bot)

        # The function that makes the ratios
        def ratio_func(tup):
            dim, mets = tup
            tgt_iter = issubclass(points_type, dict) and mets[bot].items() or mets[bot]

            # A function to get the numerator from either points_type=dict or points_type=list
            def get_top(dt):
                # Easy, just use the dict index
                if issubclass(points_type, dict):
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
    @whale_cache
    def cached_totals(cls, *args, **kwargs):
        return cls.totals(*args, **kwargs)

    @classmethod
    def total(cls, pk, metric, dimension='_', period=None, at=None, index=None,
            tzoffset=None):
        period, ats, tzoffset = Period.get_days(period, at, tzoffset=tzoffset)
        top, bot = parse_formula(metric)
        dimension = maybe_dumps(dimension)
        if not ats and not index:
            index = -1
        if isinstance(index, int):
            pps = cls.plotpoints(pk, dimension, metric, period=period, points_type=list)
            return pps[dimension][metric][index][1]
        else:
            if not bot:
                pps = cls.plotpoints(pk, dimension, metric, period=period,
                                     tzoffset=tzoffset)
                ppsm = pps[dimension][metric]
                return sum([ppsm[dt] for dt in ats if dt in ppsm])
            else:
                top_pps = cls.plotpoints(pk, dimension, top, period=period,
                                         tzoffset=tzoffset)
                bot_pps = cls.plotpoints(pk, dimension, bot, period=period,
                                         tzoffset=tzoffset)
                top_ppsm = top_pps[dimension][top]
                bot_ppsm = bot_pps[dimension][bot]
                top_tot = sum([top_ppsm[dt] for dt in ats if dt in top_ppsm])
                bot_tot = sum([bot_ppsm[dt] for dt in ats if dt in bot_ppsm])
                return bot_tot and top_tot/bot_tot or 0

    @classmethod
    def render_hw_plotpoint_table(cls, pk, metric, dimension='_', period=None, at=None,
            tzoffset=None, format=None, hidden=False, graph_color=''):
        period, ats, tzoffset = Period.get_days(period, tzoffset=tzoffset)
        top, bot = parse_formula(metric)
        pps = cls.plotpoints(pk, dimension, metric, period=period,
                             tzoffset=tzoffset)
        ppsm = pps[dimension][metric]
        if not format:
            if bot:
                format = 'pct'
            else:
                format = 'grouped'
        def fmt(v):
            import locale
            if v == 'None':
                v = None
            f = format
            if f == 'int':
                f = lambda s: int(float(s or 0))
            elif f == 'float':
                v = v or 0.0
                f = float
            elif f == 'grouped':
                v = v or 0
                f = lambda s: locale.format('%d', int(float(s)), True)
            elif f in ['pct', 'percent', '%', 'ratio']:
                v = min(101, v and float(v)*100 or 0)
                f = lambda s: '%.2f%%'%s
            elif f in ['cash', 'money', 'usd', '$', 'dollars', 'cents']:
                v = v and float(v) or 0.0
                if f == 'cents':
                    v = v/100.0
                f = locale.currency
            if not f:
                return v
            return callable(f) and f(v) or v
        hidden = hidden and 'style="display: none"' or ''
        rep = lambda s: s.format(pk=pk, metric=metric, dimension=dimension,
        hidden=hidden, color=graph_color)
        table = rep('<table {hidden} data-hw-pk="{pk}" data-hw-name="{{name}}" \
                data-hw-dimension="{dimension}" data-metric="{metric}" \
                data-hw-color="{color}">')+'\n'.join([
            '<tr><td>%s</td><td>%s</td></tr>'%(at.replace(' 00:00:00', ''), fmt(count) )
            for at, count in ppsm.items() if at in ats])+'</table>'
        return table

    @classmethod
    def today(cls, pk, metric, dimension='_'):
        return cls.total(pk, metric, dimension, Period.all_sizes()[1],
                at=cls.now())

    @classmethod
    def yesterday(cls, pk, metric, dimension='_'):
        return cls.total(pk, metric, dimension, Period.all_sizes()[1],
                at=cls.now()-timedelta(days=1))
    @classmethod
    def now(cls):
        import pytz
        if cls._local:
            return pytz.utc.localize( datetime.utcnow() )
        else:
	    return datetime.now(pytz.utc)
    @classmethod
    def now_naive(cls):
        return cls.now().replace(tzinfo=None)

    @classmethod
    def totals(cls, pk, dimensions=None, metrics=None, periods=None, tzoffset=0):
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
            period, ats, tzoffset = Period.get_days(p, tzoffset=tzoffset)
            p_data = cls.plotpoints(pk, dimensions, metrics, period=p)
            p_totals = dict()
            for dim in p_data.keys():
                p_totals[dim] = dict()
                for met, vals in p_data[dim].items():
                    p_totals[dim][met] = sum([
                        v for k, v in vals.items()
                        if k in ats])
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
        ps = MAX_INTERVALS
        print ps
        r = cls.whale_driver()
	def get_keys():
           cur = ""
	   cur, keys = r.execute_command('xscan', 'hash', "", "match", '.*||.*||.*||.*')
           for key in keys: yield key
           while cur:
	       cur, keys = r.execute_command('xscan', 'hash', cur, "match", '.*||.*||.*||.*')
               for key in keys: yield key
        for k in get_keys():
            parts = k.split('||')
            if parts == 'rank':
                continue
            try:
                val = r.hgetall(k)
            except: continue
            this_p = parts[2]
            deleted = 0
            for dt, num in val.items():
                delete = False
                if not this_p in ps:
                    delete = True
                else:
                    from dateutil import parser
                    dt_obj = parser.parse(dt)
                    expire = timedelta(seconds=ps[this_p].units[1])
                    oneday = timedelta(days=1)
                    delete = dt_obj < (cls.now_naive() - expire - oneday)
                if delete:
                    r.hdel(k, dt)
                    deleted += 1
            # Cleanup empty key
            if (len(val) - deleted) == 0:
                print 'Key empty, deleting --', k
                r.delete(k)
            elif deleted > 0:
                print 'Deleted', deleted, 'old keys from', k

    @classmethod
    def get_subdimensions(cls, pk, dimension='_'):
        if dimension == ['_']:
            dimension = '_'
        if dimension != '_' and not isinstance(dimension, list):
            dimension = [dimension]
        set_members = cls.whale_driver().smembers(keyify('subdimensions', pk,
                    dimension))
        subdimensions = []
        for s in set_members:
            loaded = try_loads(s)
            if isinstance(loaded, list) and len(loaded):
                loaded = map(unicode, loaded)
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
            except Exception, e:
                print e
        at = at or cls.now()
        if not metrics:
            metrics = ['hits']
        if type(metrics) == list:
            metrics = dict([(k, 1) for k in metrics])
        # Dimensions: {a: 5, b: {x: 1, y: 2}} --> will increment each of:
        # [_], (overall)
        # [a],
        # [a, 5],
        # [b],
        # [b, x],
        # [b, x, 1],
        # [b, y],
        # [b, y, 2]
        pipe = cls.whale_driver().pipeline(transaction=False)
        for pkk, dimension, (interval, dt, metric, i) in itertools.product(
                iterate_dimensions(pk),
                iterate_dimensions(dimensions, add_root=True),
                generate_increments(metrics, periods, at)):
            if i == 0:
                continue
            _increment(pipe, pkk, dimension, metric, interval, dt, i)
        pipe.execute()

    @classmethod
    def update_count_to(cls, pk, dimensions='_', metrics=None, period=False,
            at=False, rank=False):
        period = Period.get(period)
        at = at or cls.now()
        dt = period.flatten_str(at)
        pipe = cls.whale_driver().pipeline(transaction=False)
        for (metric, i) in metrics.iteritems():
            _store(pipe, pk, dimensions, metric, period, dt, i,
                    rank=rank)
        pipe.execute()

    @classmethod
    def zranked(cls, pk, parent_dimension='_', metric='hits', period=None,
            at=None, start=0, size=10, sort_dir=None, tzoffset=None):
        period, ats, tzoffset = Period.get_days(period, at)
        dt = ats or [Period.convert(cls.now(), tzoffset)]
        return map(try_loads,
                _ranked(cls.whale_driver(), pk, parent_dimension, metric,
                    period, dt, start, size, sort_dir=sort_dir))


    @classmethod
    def rank_subdimensions_scalar(cls, pk, dimension='_', metric='hits',
            period=None, recursive=True, prune_parents=True, points=False):
        p_obj, ats, tzoffset = Period.get_days(period)
        p_s = str(period)
        d_k = keyify(dimension)
        total = cls.totals(pk, dimension, metric, periods=[p_s])[p_s][d_k][metric]
        ranked = dict()

        def info(sub):
            pps = cls.plotpoints(pk, sub, metric, period=period)[sub][metric]
            sub_total = sum(pps.values())
            data = {
                'points': pps,
                'score': sub_total,
                'important': sub_total > 10 and (sub_total > (total / 10)),
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
        p_obj, ats, tzoffset = Period.get_days(period)
        p_s = str(p_obj)
        d_k = keyify(dimension)
        top_points = cls.totals(pk, dimension, top,
                periods=[p_s])
        top_total = p_s in top_points and top_points[p_s][d_k][top] or 0
        bottom_points = cls.totals(pk, dimension, bottom,
                periods=[p_s])
        bottom_total = p_s in bottom_points and bottom_points[p_s][d_k][bottom] or 0
        ratio_total = bottom_total and float(top_total / bottom_total) or 0
        ranked = dict()

        def info(sub):
            pps = cls.plotpoints(pk, sub, [top, bottom, '%s/%s' % (top, bottom)], period=period)[sub]
            top_pps = pps[top]
            bottom_pps = pps[bottom]

            sub_top_sum = sum({k: v for k, v in top_pps.items() if k in ats}.values())
            sub_bottom_sum = sum({k: v for k, v in bottom_pps.items() if k in ats}.values())

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
        parent_count = overall[base]['count'] or 1
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
    at = at or cls.now()
    for period in periods:
        dt = MAX_INTERVALS[period.interval].flatten_str(at)
        if dt: observations.add((period.interval, dt))
    rr = [(interval, dt, metric, incr_by)
            for (interval, dt) in observations
            for metric, incr_by in metrics.items()]
    return rr
