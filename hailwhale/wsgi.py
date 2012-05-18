import bottle
import json
import os
import hashlib
import times

from datetime import datetime
from bottle import hook, response, run, route, request as req, static_file

import util

from hail import Hail
from whale import Whale

PORT=8085
project_dir = os.path.dirname(os.path.abspath(__file__))
here = lambda * x: os.path.join(os.path.abspath(os.path.dirname(__file__)), *x)

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
    val = whale.count_now(at=times.now(), **default_params())
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
    if 'pk' not in req.GET and 'pixel' in req.GET:
        from Crypto.Cipher import AES
        from base64 import b64encode, b64decode
        from urllib import quote_plus

        key = hashlib.sha256('hailwhale_weak_key').digest()
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
            return encryptor.decrypt(b64decode(StripPadding(t, INTERRUPT, PAD)))
        params['pk'] = hw_decoded(text)
        print params['pk']
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
    period = Period.get(params['period'])
    debug = g('debug', False)
    parent_div = g('parent_div', 'hailwhale_graphs')
    hide_table = g('hide_table', False)
    height = g('height', '400px')
    params['title'] = g('title', '')
    if not params['title']:
        pkname = g('pk', '')
        dimname = util.try_loads(g('dimension', 'Overall'))
        dimname = isinstance(dimname, list) and dimname[-1] or dimname
        params['title'] = '%s [%s]' % (util.maybe_dumps(pkname), util.maybe_dumps(dimname))
    if isinstance(hide_table, basestring):
        hide_table = hide_table.lower() == 'true'
    hwurl = req.GET.get('hwurl', req.url.split('graph.js')[0])
    params['autoupdate'] = g('live', True)
    params['interval'] = g('interval', 6000)
    include_string = \
"document.write(\"<scr\" + \"ipt type='text/javascript' src='%sjs/jquery.min.js'></script>\");"%hwurl
    if hide_table: 
        table_str = '''
            $('#{parent_div}').append('<table>
                <tr>
                    <th></th>
                    <th>Dimension Name</th>
                </tr>
        '''.strip()

        dimensions = [item['dimension'] for item in Whale().rank(pk, formula).values()]
        for dimension_counter, dimension in enumerate(dimensions):
            checked = 'off'
            if dimension_counter < 10:
                checked = 'on'
            table_str += '''
                <tr>
                    <td><input id="" type="checkbox" value="{checked}" name="checkbox-{pk}-{dimension}"></td>
                    <td>{dimension}</td>
                </tr>
                '''.format(pk=pk, dimension=dimension, checked=checked)

        table_str += '''</table>');'''
    else:
        table_str = ''

    debug_return_string = '''
appended=false;\n
document.write('<div id="{id}" style="height: {height}"></div>');\n
function jqinit() {{\n
    if(typeof(jQuery) == 'undefined') {{\n
        if(!appended) {{\n
            appended = true;\n
            {include_string}\n
        }}\n
        setTimeout(jqinit, 250);\n
    }} else {{\n
        $(function() {{\n
            // Nest a few of these, very poor form \n
            $.getScript('{hwurl}js/highcharts.src.js', function() {{\n
            $.getScript('{hwurl}js/d3.js', function() {{\n
            $.getScript('{hwurl}js/hailwhale.js', function() {{\n
                $.hailwhale('{hwurl}').add_graph('{id}', {options});\n
                {table_str}
            }});\n
            }});\n
            }});\n
        }});\n
    }}
}}
jqinit();\n
    
    
    '''.format(parent_div=parent_div, include_string=include_string,
            hwurl=hwurl, table_str=table_str, height=height,
            id=hashlib.md5(str(params)).hexdigest(),
            options=util.maybe_dumps(params))
    include_string = \
"document.write(\"<scr\" + \"ipt type='text/javascript' src='%sjs/hailwhale.min.js'></script>\");"%hwurl

    return_string = '''
appended=false;\n
document.write('<div id="{id}" style="height: {height}"></div>');\n
function jqinit() {{\n
    if(typeof(jQuery) == 'undefined' || typeof(jQuery.hailwhale) == 'undefined') {{\n
        if(!appended) {{\n
            appended = true;\n
            {include_string}\n
        }}\n
        setTimeout(jqinit, 250);\n
    }} else {{\n
        $(function() {{\n
                $.hailwhale('{hwurl}').add_graph('{id}', {options});\n
                {table_str}
        }});\n
    }}
}}
jqinit();\n
    
    
    '''.format(parent_div=parent_div, include_string=include_string,
            hwurl=hwurl, table_str=table_str, height=height,
            id=hashlib.md5(str(params)).hexdigest(),
            options=util.maybe_dumps(params))
    return debug and debug_return_string or return_string


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
    run(host='0.0.0.0', port=PORT)
