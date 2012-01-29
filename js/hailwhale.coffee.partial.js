(function() {
  var $;
  var __indexOf = Array.prototype.indexOf || function(item) {
    for (var i = 0, l = this.length; i < l; i++) {
      if (this[i] === item) return i;
    }
    return -1;
  };
  $ = jQuery;
  $.hailwhale = function(host, opts) {
    this.host = host;
    this.opts = opts;
    this.make_params = function(extra) {
      var params;
      return params = {
        pk: JSON.stringify(extra.pk || extra.category || ''),
        dimensions: JSON.stringify(extra.dimensions || extra.dimension || ''),
        metrics: JSON.stringify(extra.metrics || extra.metric || ''),
        period: extra.period || ''
      };
    };
    this.trigger_fake_hits = function(extra) {
      var factor, i, params, trigger, url;
      url = this.host + '/count_now';
      params = this.make_params(extra);
      trigger = function() {
        return $.ajax({
          url: url,
          data: params,
          type: 'GET',
          success: false
        });
      };
      for (i = 1; i <= 25; i++) {
        factor = Math.floor(Math.random() * 11);
        setTimeout(trigger, 75 * i * factor);
      }
      return this;
    };
    this.add_graph = function(target, extra) {
      var params, poller, poller_handle, url;
      url = this.host + '/plotpoints';
      extra = $.extend(extra, {
        pk: extra.pk || extra.category || false,
        dimensions: extra.dimensions || extra.dimension || false,
        metrics: extra.metrics || extra.metric || false,
        metric: extra.metrics && extra.metrics[0] || extra.metric || false,
        metric_two: extra.metrics && extra.metrics[1] ? extra.metrics[1] : false,
        width_factor: extra.width_factor || 6
      });
      params = this.make_params(extra);
      params['depth'] = extra.depth || 0;
      poller = function() {
        return $.getJSON(url, params, function(data, status, xhr) {
          var colors, d_d, dimension, dimension_data, i, label, line_width, lines, max_dim, metrics, min_dim, plot, unpacked, yaxis, yaxis_two, _ref, _ref2;
          console.log(data);
          lines = [];
          colors = extra.colors || ['#000000', '#261AFF', '#0ED42F', '#E84414', '#F5E744', '#36B9FF'];
          i = 0;
          min_dim = 10;
          max_dim = 0;
          dimension_data = {};
          for (dimension in data) {
            metrics = data[dimension];
            try {
              unpacked = JSON.parse(dimension);
            } catch (error) {
              unpacked = [dimension];
            }
            if (unpacked[0] === "_") {
              unpacked = [];
            }
            if (unpacked.length < min_dim) {
              min_dim = unpacked.length;
            }
            if (unpacked.length > max_dim) {
              max_dim = unpacked.length;
            }
            dimension_data[dimension] = {
              unpacked: unpacked,
              length: unpacked.length,
              metrics: get_keys(metrics)
            };
          }
          for (dimension in data) {
            metrics = data[dimension];
            d_d = dimension_data[dimension];
            if (!extra.metric) {
              extra.metric = d_d.metrics[0];
            }
            if (!extra.metric_two && d_d.metrics.length > 1) {
              extra.metric_two = d_d.metrics[1];
            }
            if (_ref = !extra.metric, __indexOf.call(d_d.metrics, _ref) >= 0) {
              break;
            }
            if (extra.depth) {
              if (d_d.length === min_dim) {
                label = 'Overall ' + d_d.unpacked;
                line_width = extra.width_factor;
              } else {
                label = d_d.unpacked[0];
                line_width = extra.width_factor / (.5 + (d_d.length - min_dim));
              }
            } else {
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
          if (extra.metric_two) {
            yaxis = [yaxis, yaxis_two];
          }
          return plot = $.plot(target, lines, {
            legend: {
              show: !extra.hide_legend,
              position: 'sw'
            },
            xaxis: {
              mode: "time"
            },
            yaxes: yaxis
          });
        });
      };
      poller();
      if (extra.autoupdate) {
        return poller_handle = setInterval(poller, extra.interval || 1000);
      }
    };
    return this;
  };
}).call(this);
