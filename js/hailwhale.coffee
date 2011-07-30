$ = jQuery
$.hailwhale = (@host, @opts) ->
  @make_params = (extra) ->
    params =
        categories: JSON.stringify(extra.categories or extra.category or '')
        dimensions: JSON.stringify(extra.dimensions or extra.dimension or '')
        metrics: JSON.stringify(extra.metrics or extra.metric or '')
        period: extra.period or ''
  @trigger_fake_hits = (extra) ->
    url = @host + '/count_now'
    params = @make_params extra
    trigger = ->
        $.ajax({url: url, data: params, type:'GET', success: false})
    for i in [1..25]
        factor = Math.floor(Math.random()*11)
        setTimeout(trigger, 75*i*factor)
    return this
  @add_graph = (target, extra) ->
    url = @host + '/plotpoints'
    extra = $.extend extra, {
        categories: extra.categories or extra.category or false
        dimensions: extra.dimensions or extra.dimension or false
        metrics: extra.metrics or extra.metric or false
        metric: extra.metrics and extra.metrics[0] or extra.metric or false
        metric_two: if extra.metrics and extra.metrics[1] then extra.metrics[1] else false
        width_factor: extra.width_factor or 6
    }
    params = @make_params extra
    params['depth'] = extra.depth or 0
    poller = () ->
        $.getJSON url, params, (data, status, xhr) ->
            lines = []
            colors = extra.colors or ['#000000', '#261AFF','#0ED42F','#E84414','#F5E744','#36B9FF']
            #colors = ['#000000','#4B0046','#FB4A16','#F2C638','#05A6A4','#38F2F1']
            i = 0
            min_dim = 10
            max_dim = 0
            dimension_data = {}
            for dimension, metrics of data
                unpacked = JSON.parse(dimension)
                unpacked = [] if unpacked[0] == "_"
                min_dim = unpacked.length if unpacked.length < min_dim
                max_dim = unpacked.length if unpacked.length > max_dim
                dimension_data[dimension] =
                    unpacked: unpacked
                    length: unpacked.length
                    metrics: get_keys(metrics)
            for dimension, metrics of data
                d_d = dimension_data[dimension]
                if not extra.metric
                    extra.metric = d_d.metrics[0]
                if not extra.metric_two and d_d.metrics.length > 1
                    extra.metric_two = d_d.metrics[1]
                if not extra.metric in d_d.metrics then break
                if extra.depth
                    if d_d.length == min_dim
                        label = 'Overall ' + d_d.unpacked
                        line_width = extra.width_factor
                    else
                        label = d_d.unpacked[0]
                        line_width = extra.width_factor / (.5+(d_d.length - min_dim))
                else
                    label = d_d.unpacked[0] or 'Overall'
                    line_width = extra.width_factor*3/4
                lines.push {
                        data: metrics[extra.metric],
                        lines: {
                            show: true,
                            lineWidth: line_width,
                        },
                        color: colors[i++%colors.length],
                        label: label + ' ' + extra.metric,
                }
                if extra.metric_two in d_d.metrics
                    lines.push {
                        data: metrics[extra.metric_two],
                        lines: {
                            show: true,
                            lineWidth: line_width,
                        },
                        color: colors[i%colors.length],
                        label: label + ' ' + extra.metric_two,
                        yaxis: 2,
                    }
            yaxis = {min: 0}
            yaxis_two = $.extend({}, yaxis)
            yaxis_two.position = 'right'
            yaxis_two.label = extra.metric_two
            if extra.metric_two
                yaxis = [yaxis, yaxis_two]
                console.log('yaxis:',yaxis)
            plot = $.plot(target, lines, {
                legend: {show:true,position:'sw'},
                xaxis: {mode: "time"},
                yaxes: yaxis})
    poller()
    if extra.autoupdate
        poller_handle = setInterval(poller, extra.interval or 1000)
  return this
