import bottle
import json
import os

from datetime import datetime
from bottle import run, route, request as req, static_file

import util

from hail import Hail
from whale import Whale

PORT=8085
project_dir = os.path.dirname(os.path.abspath(__file__))
here = lambda * x: os.path.join(os.path.abspath(os.path.dirname(__file__)), *x)

bottle.debug(True)

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
    val = whale.count_now(at=datetime.now(), **default_params())
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

    '''

@route('/graph')
def graph():
    whale = Whale()
    points = whale.plotpoints(**default_params())
    params = {'script_tag': util.JS_TAG,
              'flotpoints': json.dumps(points),
              'random_name': 'graph_psuedorandom',
              }
    return """
<div id="%(random_name)s" style="width:97%%;height:97%%;">&nbsp;</div>
%(script_tag)s
<script type="text/javascript">
  dimensions = %(flotpoints)s;
  first_dimension = get_keys(dimensions)[0];
  first_metric = get_keys(dimensions[first_dimension])[0];
  //data = dimensions['[\"empty\"]']['hits'];
  data = dimensions[first_dimension][first_metric];
  $.plot($("#%(random_name)s"), [
    {data: data, lines: {show: true}},
  ], { xaxis: { mode: "time" } });

</script>""" % params

'''

@route('/graph.js')
def graph():
    formula = req.GET.get('metrics', 'hits')

    dimension = req.GET.get('dimension', None)

    period = req.GET.get('period', None)

    pk = req.GET.get('pk')

    parent_div = req.GET.get('parent_div', 'hailwhale_graphs')

    hide_table = req.GET.get('hide_table', False)
    if isinstance(hide_table, basestring):
        hide_table = hide_table.lower() == 'true'

    return_string = '''
        $.getScript('http://localhost:8085/js/hailwhale.complete.js', function() {{
            $('#{parent_div}').append('<div id="hailwhale-{pk}"></div>');
            hw_host = '';
            hw = $.hailwhale(hw_host);
            hw.add_graph($('#hailwhale-{pk} .plot'), {{pk: '{pk}', autoupdate: true}});
    '''.format(pk=pk, parent_div=parent_div)

    if not hide_table: 
        return_string += '''
            $('#{parent_div}').append('<table>
                <tr>
                    <th></th>
                    <th>Dimension Name</th>
                </tr>
        '''

        dimensions = [item['dimension'] for item in Whale().rank(pk, formula).values()]
        for dimension_counter, dimension in enumerate(dimensions):
            checked = 'off'
            if dimension_counter < 10:
                checked = 'on'
            return_string += '''
                <tr>
                    <td><input id="" type="checkbox" value="{checked}" name="checkbox-{pk}-{dimension}"></td>
                    <td>{dimension}</td>
                </tr>
                '''.format(pk=pk, dimension=dimension, checked=checked)

        return_string += '''</table>');'''

    return_string += '''}});'''

    return return_string


@route('/demo/:filename#.*#')
def send_static_demo(filename):
    return static_file(filename, root=here('../demo'))

@route('/js/:filename#.*#')
def send_static_js(filename):
    return static_file(filename, root=here('../js'))


if __name__ == '__main__':
    run(host='0.0.0.0', port=PORT)
