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
    val = req.GET.get(name, default)
    try: 
        val = json.loads(val)
    except Exception as e:
        pass
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

def g_tup(k, v):
    return (k, g(k, v))

def default_params():
    return dict([
            g_tup('pk', '_'), 
            g_tup('dimensions', ['_',]),
            g_tup('metrics', dict(hits=1))])


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
    params['flot_time'] = True
    return whale.plotpoints(**params)



@route('/graph.js')
def graph():
    params = {'pk': g('pk', '_'), 
            'dimension': g('dimension', '_'),
            'metric': g('metric', 'hits'),
            'depth': g('depth', 0),
            'period': g('period', '3600x86400')
            }
    pk = params['pk']
    dimension = params['dimension']
    period = params['period']
    parent_div = g('parent_div', 'hailwhale_graphs')
    hide_table = g('hide_table', False)
    params['title'] = g('title', '')
    if not params['title']:
        pkname = g('pk', '')
        dimname = g('dimension', 'Overall')
        params['title'] = '%s[%s]'%(pkname, dimname)
    length, interval = [int(part) for part in period.split('x')]

    if isinstance(hide_table, basestring):
        hide_table = hide_table.lower() == 'true'
    hwurl = req.url.split('graph.js')[0]
    params['autoupdate'] = g('live', True)
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

    return_string = '''
appended=false;\n
document.write('<div id="{id}"></div>');\n
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
            $.getScript('{hwurl}js/hailwhale.coffee.partial.js', function() {{\n
                $.hailwhale('{hwurl}').add_graph('{id}', {options});\n
                {table_str}
            }});\n
            }});\n
        }});\n
    }}
}}
jqinit();\n
    
    
    '''.format(parent_div=parent_div, include_string=include_string,
            hwurl=hwurl, table_str=table_str,
            id=hashlib.md5(str(params)).hexdigest(),
            options=json.dumps(params))
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
    run(host='0.0.0.0', port=PORT)
