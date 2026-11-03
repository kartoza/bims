let map = null;

function renderWaterTemperatureChart(){
    let startDate = $('#startDate').val();
    let endDate = $('#endDate').val();
    let url = '/api/thermal-data/?site-id='+ siteId  + '&year=' + year + '&startDate=' + startDate + '&endDate=' + endDate
    const currentParams = new URLSearchParams(window.location.search);
    const reference = currentParams.get('reference');
    const referenceCategory = currentParams.get('referenceCategory');
    if (reference) {
        url += '&reference=' + encodeURIComponent(reference);
    }
    if (referenceCategory) {
        url += '&referenceCategory=' + encodeURIComponent(referenceCategory);
    }
    fetch(url).then(
      response => response.json()
    ).then((data =>{

        drawWaterTemperatureChart(data);
    }))
}

const WATER_TEMPERATURE_SERIES = [
    {key: 'mean_7', label: 'Mean_7', color: '#000000'},
    {key: '95%_low', label: '95%_low', color: '#7f7f7f'},
    {key: '95%_up', label: '95%_up', color: '#7f7f7f'},
    {key: 'L95%_1SD', label: 'L95%_1SD', color: '#bfbfbf'},
    {key: 'U95%_1SD', label: 'U95%_1SD', color: '#bfbfbf'},
    {key: 'L95%_2SD', label: 'L95%_2SD', color: '#bfbfbf'},
    {key: 'U95%_2SD', label: 'U95%_2SD', color: '#bfbfbf'},
    {key: 'min_7', label: 'Min_7', color: '#0070c0'},
    {key: 'max_7', label: 'Max_7', color: '#ff0000'},
];

let waterTemperatureChart = null;

// Chart look and behaviour shared with the other Chart.js charts lives in
// js/utils/chartjs_highcharts_style.js (BimsChart).
function drawWaterTemperatureChart(data) {
    const container = document.getElementById('water-temperature');
    container.innerHTML = (
        '<div class="water-temperature-canvas-wrapper"><canvas></canvas></div>' +
        '<div class="water-temperature-navigator-wrapper"><canvas></canvas></div>'
    );
    const [mainCanvas, navigatorCanvas] = container.querySelectorAll('canvas');

    const chartTitle = `Water Temperature - ${year}`;
    const labels = data['date_time'];
    const toValue = v => (v === null || v === undefined || v === '') ? null : Number(v);
    const tickLabels = BimsChart.monthTickLabels(labels);

    if (waterTemperatureChart) {
        waterTemperatureChart.destroy();
    }

    waterTemperatureChart = new Chart(mainCanvas, {
        type: 'line',
        data: {
            labels: labels,
            datasets: WATER_TEMPERATURE_SERIES.map((series, index) => BimsChart.styleDataset({
                label: series.label,
                data: (data[series.key] || []).map(toValue),
            }, series.color, index)),
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            interaction: {mode: 'index', intersect: false},
            scales: {
                x: BimsChart.monthAxisOptions(tickLabels, {
                    title: {display: true, text: String(year)},
                }),
                y: {
                    title: {display: true, text: 'Water Temperature (°C)'},
                },
            },
            plugins: {
                legend: BimsChart.legendOptions('top'),
                tooltip: BimsChart.tooltipOptions({
                    title: items => new Date(items[0].label).toLocaleDateString(
                        'en-GB', {day: 'numeric', month: 'short', year: 'numeric'}),
                    label: item => `${item.dataset.label}: ${item.parsed.y}`,
                }),
            },
        },
        plugins: [BimsChart.crosshairPlugin, BimsChart.whiteBackgroundPlugin],
    });

    const timeline = BimsChart.attachNavigator(waterTemperatureChart, navigatorCanvas, {
        labels: labels,
        tickLabels: tickLabels,
        data: (data['mean_7'] || []).map(toValue),
    });

    $('#water-temperature-toolbar').show();
    $('#water-temperature-toolbar .water-temperature-range').off('click').on('click', function () {
        timeline.showLast(parseInt(this.getAttribute('data-days'), 10));
    });
    $('#water-temperature-toolbar .download-chart-png').off('click').on('click', function (e) {
        e.preventDefault();
        showDownloadPopup('CHART', chartTitle, function (downloadRequestId) {
            waterTemperatureChart.canvas.toBlob(function (blob) {
                uploadToDownloadRequest(downloadRequestId, blob, chartTitle + '.png');
            }, 'image/png');
        });
    });
    $('#water-temperature-toolbar .download-chart-csv').off('click').on('click', function (e) {
        e.preventDefault();
        showDownloadPopup('TABLE', chartTitle, function (downloadRequestId) {
            const blob = new Blob(
                [BimsChart.toCsv(waterTemperatureChart)], {type: 'text/csv'});
            uploadToDownloadRequest(downloadRequestId, blob, chartTitle + '.csv');
        });
    });
}

function changeYear(selectObject) {
  let value = selectObject.value;
  let url = new URL(window.location);
  window.location.href = `/water-temperature/${siteId}/${value}/${url.search}`;
}

function fetchThreshold() {
    let url = `/api/water-temperature-threshold/?location_site=${siteId}`;
    $.ajax({
        type: 'GET',
        url: url,
        success: function (data) {
            $('.loading-threshold').hide();
            $('.threshold_data').prop("disabled", false);
            $('#maximum_threshold').val(data['maximum_threshold'])
            $('#minimum_threshold').val(data['minimum_threshold'])
            $('#mean_threshold').val(data['mean_threshold'])
            $('#record_length').val(data['record_length'])
            $('#degree_days').val(data['degree_days'])
        },
        error: function () {
            console.error('Error fetching threshold');
        }
    })
}

$('.edit-threshold').click(function () {
    fetchThreshold();
})


$(function () {
    createDashboardMap(map, coordinates);
    renderWaterTemperatureChart()
    renderSourceReferences()
    $(".date-input").datepicker({
        changeMonth: true,
        changeYear: true,
        dateFormat: 'yy-mm-dd',
        beforeShow: function(input, inst) {
            setTimeout(function() {
                let yearSelector = $(".ui-datepicker-year");
                let currentYear = yearSelector.val();
                yearSelector.find("option").each(function() {
                    if (!availableYears.includes(parseInt($(this).val()))) {
                        $(this).remove();
                    }
                });
                if (!availableYears.includes(parseInt(currentYear))) {
                    yearSelector.val(availableYears[0]);
                }
            }, 0);
        },
        onChangeMonthYear: function(year, month, inst) {
            setTimeout(function() {
                let yearSelector = $(".ui-datepicker-year");
                yearSelector.find("option").each(function() {
                    if (!availableYears.includes(parseInt($(this).val()))) {
                        $(this).remove();
                    }
                });
            }, 0);
        }
    });

    $('#update-date').click(() => {
        let startDate = $('#startDate').val();
        let endDate = $('#endDate').val();
        let startDateYear = startDate.split('-')[0];
        let endDateYear = endDate.split('-')[0];
        if (startDateYear !== endDateYear) {
            alert('Start and end date must be within the same year');
            return;
        }
        let url = new URL(window.location);
        url.searchParams.set('startDate', startDate)
        url.searchParams.set('endDate', endDate)
        window.location.href = `/water-temperature/${siteId}/${startDateYear}/${url.search}`;
    })

    $('#threshold-form').on('submit', function(e) {
        e.preventDefault();  // Prevent the form from submitting normally
        $('.threshold_data').prop("disabled", true);
        const postData = {
            'maximum_threshold': $('#maximum_threshold').val(),
            'minimum_threshold': $('#minimum_threshold').val(),
            'mean_threshold': $('#mean_threshold').val(),
            'record_length': $('#record_length').val(),
            'degree_days': $('#degree_days').val(),
        }
        $.ajax({
            url: $(this).attr('action'),  // Get the action attribute from the form
            type: 'POST',
            headers: {"X-CSRFToken": csrfToken},
            data: postData,
            success: function(data) {
                // Redirect to the current page
                location.reload();
            },
            error: function(jqXHR, textStatus, errorThrown) {
                // Handle any errors
                console.log(errorThrown);
                location.reload()
            }
        });
    });
});
