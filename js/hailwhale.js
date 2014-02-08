(function() {
  var $,
    __indexOf = Array.prototype.indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

  $ = jQuery;
  $.charts = $.charts || [];
  var render_graph = function(target) {
        var redraw = function() {
            metric = $(target).attr('data-metric');
            source_table = $(target).attr('data-source-table');
            all_rows = $(source_table).children('tbody').children('tr');
            selectors = false; //$(target).attr('data-selector')
            selectors = selectors && selectors.split(',') || all_rows;

            if(source_table)
            {
                this_head = $('th[data-extra=graphable][data-name="'+metric+'"]', source_table)
                $('[data-extra=graph-icon]').remove();
                $(this_head).append($('<i>').attr('data-extra', 'graph-icon')
                                    .addClass('fa fa-bar-chart-o')
                    .addClass('icon').addClass('icon-signal').addClass('icon-white')
                    );
            }
            // Make clickable icons
            $('[data-graphmetric]').click(function(e) {
              clicked = $(this)
              new_metric = clicked.attr('data-graphmetric')
              if(!new_metric || new_metric == "") return;
              e.preventDefault();
              $(target).attr('data-metric', new_metric)
              $(target).attr('please-redraw', 'true')
            })
            tables = $.map(selectors, function(s) {
                return $('table[data-metric="'+metric+'"]', s)});
            current_legends = $('.legend text', target)
            datum = d3.range(tables.length).map(function(i) {
                t = tables[i]
                name = $(t).attr('data-hw-name')
                if(current_legends.length >= (i+1))
                    $(current_legends[i]).text(name);
                return {
                    key: name,
                    color: $(t).attr('data-hw-color'),
                    values: $.map($('tr', t), function(data_row) {
                        cells = $('td', data_row);
                        if(cells.length != 2)
                            return;
                        return {x: new Date(cells[0].textContent), y: parseFloat(cells[1].textContent.replace(',', '').replace('$', '').replace('%'))};})
                };
            }, tables);
            var chart = nv.models.lineChart()
                .color(d3.scale.category10().range());
            chart.xAxis
                .tickFormat(function(d) {
                  return d3.time.format('%x')(new Date(d))
                 });

            chart.yAxis
                .tickFormat(d3.format(',.1'));
            svg = $('svg', target);
            d3.select(svg[0])
                .datum(datum)
                .transition().duration(500)
                .call(chart);
            nv.utils.windowResize(chart.update);
            return chart;
        }
        redraw();
        var redraw_requests = function() {
          if($(target).attr('please-redraw'))
          {
            $(target).attr('please-redraw', null);
            redraw();
          }
        }
        setInterval(redraw_requests, 250);
    }

  $.hailwhale = function(host, opts) {
    this.host = host;
    this.opts = opts;
    var JSONify = function(str_or_obj) {
        if(typeof(str_or_obj) == "string")
            return str_or_obj;
        else
            return JSON.stringify(str_or_obj);
    }
    this.make_params = function(extra) {
      var params;
      d = new Date();
      return params = {
        pk: JSONify(extra.pk || extra.category || ''),
        dimensions: JSONify(extra.dimensions || extra.dimension || ''),
        metrics: JSONify(extra.metrics || extra.metric || ''),
        period: extra.period || '',
        tzoffset: extra.tzoffset || d.getTimezoneOffset()/60
      };
    };
    this.trigger_fake_hits = function(extra) {
      var factor, i, params, trigger, url;
      url = this.host + 'count_now';
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
    this.graph_tables = function(target, extra) {
        selector = target+'[data-hw-target]'
        graphs = $(selector)
        $.each(graphs, function(graph_idx, source_table) {
            columns = $(source_table).children('thead').first().children('tr').first().children('th[data-extra*=graphable]')
            rows =  $(source_table).children('tbody').first().children('tr')
            graph_selector = $(source_table).attr('data-hw-target');
            graph = $(graph_selector)
            row_ids = $.map(rows, function(r) {
                $(r).attr('data-id', $(r).attr('id'));
                return '[data-id="' + $(r).attr('id')+'"]'});
            $(graph_selector).attr('data-selector', row_ids.join(','));
            $(graph_selector).attr('data-source-table', '#'+$(source_table).attr('id'));
            if(!$(source_table).attr('data-metric'))
                $(graph_selector).attr('data-metric', $(columns[0]).attr('data-name'));
            else
                $(graph_selector).attr('data-metric', $(source_table).attr('data-metric'));
            graph_obj = render_graph(graph_selector);
        });
    }
    this.add_graph = function(target, extra) {
      var params, poller, poller_handle, url;

      // Get the jquery object of the target
      if(typeof(target) == 'string' && target[0] != '#')
          target='#'+target;
      target = $(target)[0];

      var charts_on_page = $.charts.length || 0;
      var our_chart_id = charts_on_page+1;
      var our_chart = $.charts[our_chart_id];
      var w = $(target).width(),
          h = $(target).height();
      $.charts.push(our_chart);
      extra = $.extend(extra, {
        pk: extra.pk || extra.category || false,
        dimensions: extra.dimensions || extra.dimension || false,
        metric: extra.metrics && extra.metrics[0] || extra.metric || false,
        metric_two: extra.metrics && extra.metrics[1] ? extra.metrics[1] : false,
        url: extra.url || false,
        area: extra.area || false
      });
      params = this.make_params(extra);
      if(extra.area) {
        if(extra.area == 'wiggle' || extra.area == 'expand' || extra.area == 'zero' || extra.area == 'silhouette')
        {
          extra.d3 = extra.area;
          var stack_func = d3.layout.stack().offset(extra.d3);
          var area = d3.svg.area();
          our_chart = $.charts[our_chart_id] = d3.select(target).append("svg");
        }
      }
    };
    return this;
  };

}).call(this);
