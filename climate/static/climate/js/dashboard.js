(function () {
  'use strict';

  var dataEl = document.getElementById('climate-dashboard-data');
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

  // Chart look and behaviour (symbols, legend, tooltip, timeline) is shared
  // with the other Chart.js charts in bims/static/js/utils/chartjs_highcharts_style.js.

  var charts = {
    temperature: null,
    humidity: null,
    rainfall: null
  };

  var rainfallTitles = {
    daily: 'Daily total rainfall',
    monthly: 'Monthly total rainfall',
    annual: 'Annual total rainfall'
  };

  function destroyChart(entry) {
    if (!entry) {
      return;
    }
    entry.main.destroy();
    if (entry.timeline) {
      entry.timeline.chart.destroy();
    }
  }

  /**
   * Adds a Download (PNG / CSV) menu above a chart container, once. The menu
   * always exports whichever chart is currently stored under `name`.
   */
  function addDownloadMenu(container, name, title) {
    if (container.getAttribute('data-download-menu')) {
      return;
    }
    container.setAttribute('data-download-menu', 'true');
    var toolbar = document.createElement('div');
    toolbar.className = 'text-right mb-2';
    toolbar.innerHTML = (
      '<div class="btn-group">' +
      '<button type="button" class="btn btn-sm btn-outline-secondary dropdown-toggle" ' +
      'data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">' +
      '<i class="fa fa-download" aria-hidden="true"></i> Download</button>' +
      '<div class="dropdown-menu dropdown-menu-right">' +
      '<a class="dropdown-item" href="#" data-format="png">PNG image</a>' +
      '<a class="dropdown-item" href="#" data-format="csv">CSV</a>' +
      '</div></div>'
    );
    container.parentNode.insertBefore(toolbar, container);

    var filename = title.replace(/[^a-z0-9]/gi, '_').toLowerCase();
    var runExport = function (format) {
      var entry = charts[name];
      if (!entry) {
        return;
      }
      if (format === 'csv') {
        BimsChart.downloadBlob(
          new Blob([BimsChart.toCsv(entry.main, 'Period')], {type: 'text/csv'}),
          filename + '.csv');
      } else {
        entry.main.canvas.toBlob(function (blob) {
          BimsChart.downloadBlob(blob, filename + '.png');
        }, 'image/png');
      }
    };
    toolbar.addEventListener('click', function (event) {
      var item = event.target.closest('[data-format]');
      if (!item) {
        return;
      }
      event.preventDefault();
      var format = item.getAttribute('data-format');
      if (typeof showDownloadPopup === 'function') {
        showDownloadPopup('IMAGE', title, function () {
          runExport(format);
        }, true, null, false);
      } else {
        runExport(format);
      }
    });
  }

  /**
   * Builds a main chart (and optionally a timeline under it) inside
   * `container`, replacing whatever was drawn there before.
   */
  function drawChart(name, containerId, title, config) {
    var container = document.getElementById(containerId);
    if (!container) {
      return;
    }
    destroyChart(charts[name]);
    charts[name] = null;
    addDownloadMenu(container, name, title);

    container.style.height = 'auto';
    container.innerHTML = (
      '<div style="position: relative; height: ' + config.height + 'px;"><canvas></canvas></div>' +
      (config.timelineData
        ? '<div style="position: relative; height: 90px; margin-top: 10px;"><canvas></canvas></div>'
        : '')
    );
    var canvases = container.querySelectorAll('canvas');
    var main = new Chart(canvases[0], config.chart);
    var timeline = null;
    if (config.timelineData) {
      timeline = BimsChart.attachNavigator(main, canvases[1], {
        labels: config.chart.data.labels,
        data: config.timelineData,
        spanGaps: config.timelineSpanGaps
      });
    }
    charts[name] = {main: main, timeline: timeline};
  }

  function lineChartConfig(labels, series, yTitle, granularity) {
    return {
      type: 'line',
      data: {
        labels: labels,
        datasets: series.map(function (item, index) {
          var dataset = BimsChart.styleDataset({
            label: item.name,
            data: item.data
          }, item.color, index);
          // Markers are only drawn permanently for the coarser views.
          dataset.pointRadius = granularity === 'daily' ? 0 : 3;
          dataset.spanGaps = true;
          return dataset;
        })
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        animation: false,
        interaction: {mode: 'index', intersect: false},
        scales: {
          x: {
            ticks: {autoSkip: true, minRotation: 45, maxRotation: 45, font: {size: 10}}
          },
          y: {
            title: {display: true, text: yTitle}
          }
        },
        plugins: {
          legend: BimsChart.legendOptions('bottom'),
          tooltip: BimsChart.tooltipOptions({
            label: function (item) {
              return item.dataset.label + ': ' + item.parsed.y;
            }
          })
        }
      },
      plugins: [BimsChart.crosshairPlugin, BimsChart.whiteBackgroundPlugin]
    };
  }

  function renderCharts(granularity) {
    var data = allData[granularity];
    if (!data || !data.labels || !data.labels.length) {
      return;
    }

    // Update rainfall chart title
    var rainfallTitle = document.getElementById('rainfall-chart-title');
    if (rainfallTitle) {
      rainfallTitle.textContent = rainfallTitles[granularity] || 'Total rainfall';
    }

    if (data.temperature) {
      drawChart('temperature', 'temperature-chart', 'Temperature (°C)', {
        height: 310,
        chart: lineChartConfig(data.labels, [
          {name: 'Min (°C)', data: data.temperature.min, color: '#3F51B5'},
          {name: 'Ave (°C)', data: data.temperature.avg, color: '#009688'},
          {name: 'Max (°C)', data: data.temperature.max, color: '#E91E63'}
        ], 'Temperature (°C)', granularity),
        timelineData: data.temperature.min
      });
    }

    if (data.humidity) {
      drawChart('humidity', 'humidity-chart', 'Humidity (%)', {
        height: 310,
        chart: lineChartConfig(data.labels, [
          {name: 'Min (%)', data: data.humidity.min, color: '#5C6BC0'},
          {name: 'Ave (%)', data: data.humidity.avg, color: '#26A69A'},
          {name: 'Max (%)', data: data.humidity.max, color: '#FF7043'}
        ], 'Humidity (%)', granularity),
        timelineData: data.humidity.min
      });
    }

    if (data.rainfall) {
      var rainfallSeries = [];

      if (granularity === 'monthly' && data.rainfall.historical) {
        rainfallSeries.push({
          label: 'Monthly average over all time',
          data: data.rainfall.historical,
          backgroundColor: '#146082'
        });
      }

      rainfallSeries.push({
        label: 'Average monthly total rainfall (for dates selected)',
        data: data.rainfall.total,
        backgroundColor: '#E97132'
      });

      rainfallSeries.forEach(function (dataset) {
        dataset.borderRadius = 3;
      });

      drawChart('rainfall', 'rainfall-chart', 'Rainfall (mm)', {
        height: 260,
        chart: {
          type: 'bar',
          data: {labels: data.labels, datasets: rainfallSeries},
          options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            interaction: {mode: 'index', intersect: false},
            scales: {
              x: {
                ticks: {autoSkip: true, minRotation: 45, maxRotation: 45, font: {size: 10}}
              },
              y: {
                beginAtZero: true,
                title: {display: true, text: 'Rainfall (mm)'}
              }
            },
            plugins: {
              legend: BimsChart.legendOptions('bottom', 'bar'),
              tooltip: BimsChart.tooltipOptions({
                label: function (item) {
                  return item.dataset.label + ': ' + item.parsed.y;
                }
              }, 'bar')
            }
          },
          plugins: [BimsChart.whiteBackgroundPlugin]
        },
        // The timeline helps with long daily series; shorter ones fit on screen.
        timelineData: data.labels.length > 60 ? data.rainfall.total : null,
        // Bars have no line to break, so connect the overview across gaps.
        timelineSpanGaps: true
      });
    }
  }

  renderCharts('monthly');
  var granularitySelect = document.getElementById('granularity-select');
  if (granularitySelect) {
    granularitySelect.addEventListener('change', function () {
      renderCharts(this.value);
    });
  }
})();
