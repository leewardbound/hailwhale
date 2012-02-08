console.log('hello');
var chart; // global
var request_interval = 60*1000;
var colors = ['#000000', '#261AFF', '#0ED42F', '#E84414', '#F5E744', '#36B9FF'];
var extra = {};
function requestData() {
  console.log('requesting data');
	$.ajax({
		url: '/plotpoints',
		dataType: 'json',
		success: receiveData,
		cache: false
	});
}

function receiveData(data) {
  console.log('got data');
  // Data comes back as JSON in the following format:
  // { 'dimension_str_or_list': {'metric': [[x, y], ... ] , 'metric2': ...} }
  // First, let's look at the dimensions and gather some info
  var min_dim, max_dim = 0;
  var dimension_data = {};
  for (dimension in data) {
    metrics = data[dimension];

    // Decipher dimension's name
    try {
      unpacked = JSON.parse(dimension);
    } catch (error) {
      unpacked = [dimension];
    }
    if (unpacked[0] === "_") unpacked = [];
    if (unpacked.length < min_dim) min_dim = unpacked.length;
    if (unpacked.length > max_dim) max_dim = unpacked.length;

    // Keep some extra info about each dimension around
    dimension_data[dimension] = {
      unpacked: unpacked,
      length: unpacked.length,
      metrics: metrics
    };
  }

  // OK, now if any of the dimensions changed, we have to re-render the graph
  var re_render = false;
  if(!chart)
      re_render = true;
  else
  {
      for(dimension in data)
          if(!chart.get(dimension))
              re_render = true;
      for(index in chart.series)
          if(!data[chart.series[index]])
              re_render = true;
  }

  if(re_render) {
      console.log('re-drawing graph');
      var render_options = {
        chart: {
          renderTo: 'hailwhale-test', // don't hardcode this
          defaultSeriesType: 'spline',
        },
        title: {
          text: 'hailwhale'
        },
        xAxis: {
          type: 'datetime',
          tickPixelInterval: 150,
          maxZoom: 20 * 1000
        },
        yAxis: {
          minPadding: 0.2,
          maxPadding: 0.2,
          title: {
            margin: 20
          }
        },
        series: []
      }

      // Now we loop dimensions and find our relevant plotpoints
      for (dimension in data) {
        metrics = data[dimension];
        d_d = dimension_data[dimension];

        // If no metric specified, default to the first one
        if (!extra.metric) extra.metric = d_d.metrics[0];

        // If two metrics returned, show the second one too!
        if (!extra.metric_two && d_d.metrics.length > 1) {
          extra.metric_two = d_d.metrics[1];
        }

        if (_ref = !extra.metric, __indexOf.call(d_d.metrics, _ref) >= 0) {
          break;
        }
        // Did we ask for a nested graph? 
        if (extra.depth) {
          // If so, let's give parent a fat line and all the kids skiny ones
          if (d_d.length === min_dim) {
            label = 'Overall ' + d_d.unpacked;
            line_width = extra.width_factor;
          } else {
            label = d_d.unpacked[0];
            line_width = extra.width_factor / (.5 + (d_d.length - min_dim));
          }
        } else {
          // Not nested graphs, just size these all the same
          label = d_d.unpacked[0] || 'Overall';
          line_width = extra.width_factor * 3 / 4;
        }

        lines.push({
          data: metrics[extra.metric],
          lines: {
            show: true,
            lineWidth: line_width
          },
          color: colors[i++ % colors.length],
          label: label + ' ' + extra.metric
        });
        if (_ref2 = extra.metric_two, __indexOf.call(d_d.metrics, _ref2) >= 0) {
          lines.push({
            data: metrics[extra.metric_two],
            lines: {
              show: true,
              lineWidth: line_width
            },
            color: colors[i % colors.length],
            label: label + ' ' + extra.metric_two,
            yaxis: 2
          });
        }
      }
      yaxis = {
        min: 0
      };
      yaxis_two = $.extend({}, yaxis);
      yaxis_two.position = 'right';
      yaxis_two.label = extra.metric_two;
      if (extra.metric_two) yaxis = [yaxis, yaxis_two];
  }

  // Now update the plotpoints!
  $.each(chart.series, function(index, series) {
    points = points_data['_']['hits']
    shift = series.data.length > 20; // shift if the series is longer than 20
    for(p in points) {
        series.addPoint(points[p], true, shift);
    }
  });
  
  // call it again to update
  setTimeout(requestData, request_interval);	
}
	
function hailwhale_graph(title, length, interval) {
  console.log('doing graph');
  if(typeof(interval) != 'undefined')
      request_interval = Math.min(interval/2, 60)*1000;
  requestData();
}
