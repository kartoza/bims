(function () {
  'use strict';

  var dataEl = document.getElementById('groundwater-dashboard-data');
  if (!dataEl) {
    return;
  }

  var allData;
  try {
    allData = JSON.parse(dataEl.textContent);
  } catch (err) {
    console.error('Unable to parse dashboard payload', err);
    return;
  }
  if (!allData) {
    return;
  }

  var charts = {};

  function pairs(timestamps, values) {
    var result = [];
    for (var i = 0; i < timestamps.length; i++) {
      result.push([timestamps[i], values[i]]);
    }
    return result;
  }

  function render(containerId, yTitle, series) {
    var container = document.getElementById(containerId);
    if (!container) {
      return;
    }
    if (charts[containerId]) {
      charts[containerId].destroy();
    }
    charts[containerId] = Highcharts.stockChart(containerId, {
      chart: {type: 'line', spacingTop: 40},
      title: {text: ''},
      rangeSelector: {enabled: false},
      navigator: {enabled: true},
      scrollbar: {enabled: true},
      xAxis: {type: 'datetime', title: {text: ''}},
      // Water level is a depth below ground, so the axis is reversed
      yAxis: {title: {text: yTitle}, opposite: false, reversed: containerId === 'level-chart'},
      legend: {enabled: series.length > 1},
      tooltip: {valueDecimals: 2, shared: true},
      series: series
    });
  }

  function renderCharts(granularity) {
    var data = allData[granularity];
    if (!data || !data.timestamps || !data.timestamps.length) {
      return;
    }
    var t = data.timestamps;
    var levelSeries = [{name: 'Average', data: pairs(t, data.level.avg), color: '#1f6fb4'}];
    if (granularity !== 'hourly') {
      levelSeries.unshift({name: 'Minimum', data: pairs(t, data.level.min), color: '#9ecae1', dashStyle: 'ShortDash'});
      levelSeries.push({name: 'Maximum', data: pairs(t, data.level.max), color: '#08306b', dashStyle: 'ShortDash'});
    }
    render('level-chart', 'Water level (m below ground)', levelSeries);
    render('temperature-chart', 'Water temperature (°C)',
      [{name: 'Temperature', data: pairs(t, data.temperature), color: '#d6604d'}]);
    render('conductivity-chart', 'Conductivity (µS/cm)',
      [{name: 'Conductivity', data: pairs(t, data.conductivity), color: '#4d9221'}]);
  }

  var select = document.getElementById('granularity-select');
  renderCharts(select ? select.value : 'daily');
  if (select) {
    select.addEventListener('change', function () {
      renderCharts(this.value);
    });
  }
})();
