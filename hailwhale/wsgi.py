import bottle
import json
import os
import hashlib
import times

from datetime import datetime
from bottle import hook, response, run, route, request as req, static_file

PORT=80
project_dir = os.path.dirname(os.path.abspath(__file__))
here = lambda * x: os.path.join(os.path.abspath(os.path.dirname(__file__)), *x)
import sys
sys.path.insert(0, project_dir)

import util

from hail import Hail
from whale import Whale


bottle.debug(True)
@hook('after_request')
def enable_cors():
    response.headers['Access-Control-Allow-Origin'] = '*'

def g(name, default=None, coerce_to=True):
    val = util.try_loads(req.GET.get(name, default))
    try:
        if coerce_to is True:
            coerce_to = type(default)
        if coerce_to:
            if coerce_to is list and isinstance(val, basestring):
                val = [val, ]
            elif coerce_to is list and type(val) == dict:
                pass
            else:
                val = coerce_to(val)
    except Exception as e:
        pass
    if val in ['', [], {}, '""', '\"\"', [u''], [u'""']]:
        return default
    return val

def g_tup(k, v, coerce_to=True):
    return (k, g(k, v, coerce_to))

def default_params():
    return dict([
            g_tup('pk', '_', False),
            g_tup('dimensions', ['_'], False),
            g_tup('metrics', dict(hits=1), False)])


@route('/count')
def count():
    hail = Hail()
    val = hail.count(**default_params())
    return 'OK'

@route('/reset')
def reset():
    whale = Whale()
    try:
        whale.reset(**default_params())
    except Exception as e:
        return str(e)
    return 'OK'

@route('/count_now')
def count_now():
    whale = Whale()
    at = g('at', False)
    tzoffset = None
    if not at:
        at = times.now()
    else:
        from dateutil.parser import parse
        at = parse(g('at'))
        at = at.replace(tzinfo=None)
    val = whale.count_now(at=at, **default_params())
    return 'OK'

@route('/update_count_to')
def update_count_to():
    whale = Whale()
    at = g('at', False)
    tzoffset = None
    if not at:
        at = times.now()
    else:
        from dateutil.parser import parse
        at = parse(g('at'))
        at = at.replace(tzinfo=None)
    params = dict(at=at,
            period=g('period', None),
            **default_params())

    val = whale.update_count_to(**params)
    return 'OK'

@route('/flush_hail')
def flush_hail():
    hail = Hail()
    try:
        hail.dump_now()
    except Exception as e:
        return str(e)
    return 'OK'

@route('/totals')
def totals():
    whale = Whale()
    params = default_params()
    if type(params['metrics']) == dict:
        params['metrics'] = params['metrics'].keys()
    return whale.totals(**params)

@route('/plotpoints')
def plotpoints():
    whale = Whale()
    params = default_params()
    params['depth'] = g('depth', 0)
    params['period'] = g('period', None)
    params['sort'] = g('sort', None)
    params['limit'] = g('limit', 10)
    params['tzoffset'] = g('tzoffset', 0.0)
    params['flot_time'] = True
    return whale.plotpoints(**params)


@route('/tracker')
def tracker():
    from periods import Period
    import random
    params = default_params()
    # LOLOL THIS SHOULD REALLY CHANGE
    key = hashlib.sha256('hailwhale_weak_key').digest()
    if 'pk' not in req.GET and 'pixel' in req.GET:
        from Crypto.Cipher import AES
        from base64 import b64encode, b64decode
        from urllib import quote_plus

        mode = AES.MODE_CBC
        encryptor = AES.new(key, mode)
        text = g('pixel')
        INTERRUPT = u'\u0001'
        PAD = u'\u0000'

        # Since you need to pad your data before encryption,
        # create a padding function as well
        # Similarly, create a function to strip off the padding after decryption
        def AddPadding(data, interrupt, pad, block_size):
            new_data = ''.join([data, interrupt])
            new_data_len = len(new_data)
            remaining_len = block_size - new_data_len
            to_pad_len = remaining_len % block_size
            pad_string = pad * to_pad_len
            return ''.join([new_data, pad_string])
        def StripPadding(data, interrupt, pad):
            return data.rstrip(pad).rstrip(interrupt)
        def hw_encoded(t):
            return quote_plus(b64encode(encryptor.encrypt(AddPadding(t, INTERRUPT, PAD, 32))))
        def hw_decoded(t):
            return StripPadding(encryptor.decrypt(b64decode(t)), INTERRUPT, PAD)
        params['pk'] = hw_decoded(text)
    pk = params['pk']
    whale = Whale()
    hail = Hail()
    val = whale.count_now(at=times.now(), **params)
    uid = g('uid')
    if not uid or uid == '_new':
        default = random.randrange(10**6,10**9)
        uid = str(req.get_cookie('uid', str(default), key))
    hail.spy_log(uid, params)
    response.set_cookie('uid', uid, key)
    return str(uid)


@route('/table_graph.js')
def table_graph():
    from periods import Period
    params = {
            'tzoffset': g('tzoffset', 0.0),
            'period': g('period', str(Period.get(None))),
            }
    debug = g('debug', False)
    table = g('table', '')
    height = g('height', '300px')
    delay = g('delay', 5000)
    hwurl = req.GET.get('hwurl', '/' or req.url.split('table_graph.js')[0])
    include_string = \
"document.write(\"<scr\" + \"ipt type='text/javascript' src='%sjs/jquery.min.js'></script>\");"%hwurl
    include_string += \
"document.write(\"<scr\" + \"ipt type='text/javascript' src='%sjs/hailwhale.js'></script>\");"%hwurl
    include_string += \
"document.write(\"<scr\" + \"ipt type='text/javascript' src='%sjs/d3.js'></script>\");"%hwurl
    include_string += \
"document.write(\"<scr\" + \"ipt type='text/javascript' src='%sjs/nvd3.js'></script>\");"%hwurl

    return_string = '''
appended=false;\n
function jqinit() {{\n
    if(typeof(jQuery) == 'undefined' || typeof(jQuery.hailwhale) == 'undefined') {{\n
        if(!appended) {{\n
            appended = true;\n
            {include_string}\n
        }}\n
        setTimeout(jqinit, 250);\n
    }} else {{\n
        $(function() {{\n
        init_graphs =function() {{
                $.hailwhale('{hwurl}').graph_tables('{table}', {options});\n
                }}
        setTimeout(init_graphs, {delay});
        if(ui_loaded_funcs)
            ui_loaded_funcs.init_graphs = init_graphs;
        }});\n
    }}
}}
jqinit();\n


    '''.format( include_string=include_string, table=table, delay=delay,
            hwurl=hwurl, options=util.maybe_dumps(params))
    return return_string


@route('/graph.js')
def graph():
    from periods import Period
    params = {'pk': g('pk', '_', False),
            'dimension': g('dimension', '_', False),
            'metric': g('metric', 'hits', False),
            'depth': g('depth', 0),
            'tzoffset': g('tzoffset', 0.0),
            'period': g('period', str(Period.get(None))),
            'area': g('area', ''),
            }
    pk = params['pk']
    dimension = params['dimension']
    metric = params['metric']
    period = Period.get(params['period'])
    debug = g('debug', False)
    parent_div = g('parent_div', 'hailwhale_graphs')
    table = g('table', False)
    height = g('height', '300px')
    params['title'] = g('title', '')
    if not params['title']:
        pkname = g('pk', '')
        dimname = util.try_loads(g('dimension', 'Overall'))
        dimname = isinstance(dimname, list) and dimname[-1] or dimname
        params['title'] = '%s [%s]' % (util.maybe_dumps(pkname), util.maybe_dumps(dimname))
    if isinstance(table, basestring):
        table = table.lower() == 'true'
    hwurl = req.GET.get('hwurl', req.url.split('graph.js')[0])
    params['autoupdate'] = g('live', True)
    params['interval'] = g('interval', 6000)
    graph_id = hashlib.md5(str(params)).hexdigest()
    include_string = \
"document.write(\"<scr\" + \"ipt type='text/javascript' src='%sjs/jquery.min.js'></script>\");"%hwurl
    if table:
        try:
            columns = int(g('table', 6, int))
        except:
            columns = 6
        pps = Whale.plotpoints(pk, dimension, metric, period=period,
                depth=params['depth'])
        dates = [p for p in
                Period.get(period).datetimes_strs()][(-1*columns - 1):]

        table_str = '''
            $('#{id} .table').html('<table style="width: 100%"> <tr> <th></th> <th></th> {columns} </tr>
        '''.strip().format(id=graph_id,columns=' '.join([
            '<th>%s</th>'%date.replace('00:00:00 ', '') for date in dates]))

        dimensions = pps.keys()
        if '_' in dimensions:
            dimensions.remove('_')
            dimensions = ['_'] + dimensions
        for dimension_counter, dimension in enumerate(dimensions):
            checked = 'off'
            if dimension_counter < 10:
                checked = 'on'
            if dimension == '_':
                if params['depth']:
                    continue
                dimension_name = '<b>Overall</b>'
            else:
                dimension_name = dimension.capitalize()
            table_str += '''
                <tr> <td><input id="" style="display: none" type="checkbox" value="{checked}" name="checkbox-{pk}-{dimension}"></td> <td>{dimension_name}</td> {columns} </tr>
                '''.format(pk=pk, dimension=dimension, checked=checked,
                        dimension_name=dimension_name,
                        columns=' '.join([
                "<td>%s</td>"%int(pps[dimension][metric][date]) for date in dates])).strip()

        table_str += '''</table>');'''
    else:
        table_str = ''
    include_string = \
"document.write(\"<scr\" + \"ipt type='text/javascript' src='%sjs/hailwhale.min.js'></script>\");"%hwurl

    return_string = '''
appended=false;\n
document.write('<div id="{id}"><div class="graph" style="height: {height}"></div><div class="table"></div></div>');\n
function jqinit() {{\n
    if(typeof(jQuery) == 'undefined' || typeof(jQuery.hailwhale) == 'undefined') {{\n
        if(!appended) {{\n
            appended = true;\n
            {include_string}\n
        }}\n
        setTimeout(jqinit, 250);\n
    }} else {{\n
        $(function() {{\n
                $.hailwhale('{hwurl}').add_graph('{id} .graph', {options});\n
                {table_str}
        }});\n
    }}
}}
jqinit();\n


    '''.format(parent_div=parent_div, include_string=include_string,
            hwurl=hwurl, table_str=table_str, height=height,
            id=graph_id,
            options=util.maybe_dumps(params))
    return return_string


@route('/demo/:filename#.*#')
def send_static_demo(filename):
    return static_file(filename, root=here('../demo'))

@route('/js/:filename#.*#')
def send_static_js(filename):
    return static_file(filename, root=here('../js'))

@route('/autographs/:filename#.*#')
def send_static_js(filename):
    return static_file(filename, root=here('../autographs'))


if __name__ == '__main__':
    host, port = len(sys.argv) > 1 and sys.argv[1].split(':') or ("", "")
    host, port = (host or '0.0.0.0', port or PORT)
    run(host=host, port=port)
application = bottle.default_app()
