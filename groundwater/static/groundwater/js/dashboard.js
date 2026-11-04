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

  var toValue = function (v) {
    return (v === null || v === undefined || v === '') ? null : Number(v);
  };

  function formatTitle(label, granularity) {
    var d = new Date(label);
    var opts = {day: 'numeric', month: 'short', year: 'numeric'};
    if (granularity === 'monthly') {
      opts = {month: 'short', year: 'numeric'};
    } else if (granularity === 'hourly') {
      opts.hour = '2-digit';
      opts.minute = '2-digit';
    }
    return d.toLocaleString('en-GB', opts);
  }

  function render(containerId, yTitle, series, labels, granularity) {
    var container = document.getElementById(containerId);
    if (!container) {
      return;
    }
    if (charts[containerId]) {
      charts[containerId].navigator.chart.destroy();
      charts[containerId].main.destroy();
    }
    container.style.height = 'auto';
    container.innerHTML =
      '<div style="position:relative;width:100%;height:300px;"><canvas></canvas></div>' +
      '<div style="position:relative;width:100%;height:70px;margin-top:10px;"><canvas></canvas></div>';
    var canvases = container.querySelectorAll('canvas');
    var tickLabels = BimsChart.monthTickLabels(labels);

    var main = new Chart(canvases[0], {
      type: 'line',
      data: {
        labels: labels,
        datasets: series.map(function (s, index) {
          var dataset = BimsChart.styleDataset({
            label: s.name,
            data: s.data.map(toValue)
          }, s.color, index);
          if (s.dashed) {
            dataset.borderDash = [6, 4];
          }
          return dataset;
        })
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        interaction: {mode: 'index', intersect: false},
        scales: {
          x: BimsChart.monthAxisOptions(tickLabels),
          // Water level is a depth below ground, so the axis is reversed
          y: {title: {display: true, text: yTitle}, reverse: containerId === 'level-chart'}
        },
        plugins: {
          legend: Object.assign(BimsChart.legendOptions('top'), {display: series.length > 1}),
          tooltip: BimsChart.tooltipOptions({
            title: function (items) {
              return formatTitle(items[0].label, granularity);
            },
            label: function (item) {
              var y = item.parsed.y;
              return item.dataset.label + ': ' + (y === null ? '-' : y.toFixed(2));
            }
          })
        }
      },
      plugins: [BimsChart.crosshairPlugin, BimsChart.whiteBackgroundPlugin]
    });
    var navigator = BimsChart.attachNavigator(main, canvases[1], {
      labels: labels,
      tickLabels: tickLabels,
      data: series[Math.floor(series.length / 2)].data.map(toValue)
    });
    charts[containerId] = {main: main, navigator: navigator};
  }

  function renderCharts(granularity) {
    var data = allData[granularity];
    if (!data || !data.timestamps || !data.timestamps.length) {
      return;
    }
    var labels = data.timestamps.map(function (ts) {
      return new Date(ts).toISOString();
    });
    var levelSeries = [{name: 'Average', data: data.level.avg, color: '#1f6fb4'}];
    if (granularity !== 'hourly') {
      levelSeries.unshift({name: 'Minimum', data: data.level.min, color: '#9ecae1', dashed: true});
      levelSeries.push({name: 'Maximum', data: data.level.max, color: '#08306b', dashed: true});
    }
    render('level-chart', 'Water level (m below ground)', levelSeries, labels, granularity);
    render('temperature-chart', 'Water temperature (°C)',
      [{name: 'Temperature', data: data.temperature, color: '#d6604d'}], labels, granularity);
    render('conductivity-chart', 'Conductivity (µS/cm)',
      [{name: 'Conductivity', data: data.conductivity, color: '#4d9221'}], labels, granularity);
  }

  var select = document.getElementById('granularity-select');
  renderCharts(select ? select.value : 'daily');
  if (select) {
    select.addEventListener('change', function () {
      renderCharts(this.value);
    });
  }
})();
