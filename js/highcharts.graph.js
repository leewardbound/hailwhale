$.getScript('http://www.highcharts.com/js/highcharts.src.js', function() {



var chart; // global

console.log('highcharts js loaded');
		
function requestData() {
	$.ajax({
		url: 'data.json',
		dataType: 'json',
		success: function(points_data) {
			$.each(chart.series, function(index, series) {
				shift = series.data.length > 20; // shift if the series is longer than 20
				series.addPoint(points_data[series.name], true, shift);
			});
			
			// call it again after one second
			setTimeout(requestData, 1000);	
		},
		cache: false
	});
}
	
function hailwhale_graph(title, length, interval) {
	console.log('graphing function called');
	chart = new Highcharts.Chart({
		chart: {
			renderTo: 'hailwhale-test',
			defaultSeriesType: 'spline',
			events: {
				load: requestData
			}
		},
		title: {
			text: title
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
		series: [{
			name: 'Random 1',
			data: []
		},
		{
			name: 'Random 2',
			data: []
		}]
	});		
}

});