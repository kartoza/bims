let map = null;

function drawMap(data) {
    let scaleLineControl = new ol.control.ScaleLine();
    const baseLayer = [];
    let basemap = (typeof BASEMAP_CONFIG !== 'undefined' ? BASEMAP_CONFIG : null) || {};
    if (basemap['source_type'] === 'map_tiler' && basemap['url'] && basemap['key']) {
        baseLayer.push(new ol.layer.Tile({
            source: new ol.source.TileJSON({
                url: basemap['url'] + '?key=' + basemap['key'],
                tileSize: 512,
                crossOrigin: 'anonymous'
            })
        }));
    } else {
        baseLayer.push(new ol.layer.Tile({
            source: new ol.source.OSM({wrapX: false, noWrap: true})
        }));
    }

    map = new ol.Map({
        controls: ol.control.defaults().extend([scaleLineControl]),
        layers: baseLayer,
        target: 'map',
        view: new ol.View({center: [0, 0], zoom: 2})
    });

    let graticule = new ol.Graticule({
        strokeStyle: new ol.style.Stroke({
            color: 'rgba(0,0,0,1)',
            width: 1,
            lineDash: [2.5, 4]
        }),
        showLabels: true
    });
    graticule.setMap(map);

    let iconFeatures = [];
    $.each(data['coordinates'], function (index, coordinate) {
        let iconFeature = new ol.Feature({
            geometry: new ol.geom.Point(
                ol.proj.transform([coordinate['x'], coordinate['y']], 'EPSG:4326', 'EPSG:3857')
            )
        });
        iconFeatures.push(iconFeature);
    });

    let vectorSource = new ol.source.Vector({features: iconFeatures});
    let iconStyle = new ol.style.Style({
        image: new ol.style.Icon({
            anchor: [0.5, 46],
            anchorXUnits: 'fraction',
            anchorYUnits: 'pixels',
            opacity: 0.75,
            src: '/static/img/map-marker.png'
        })
    });
    let vectorLayer = new ol.layer.Vector({source: vectorSource, style: iconStyle});
    map.addLayer(vectorLayer);

    if (iconFeatures.length > 0) {
        map.getView().fit(vectorSource.getExtent(), map.getSize());
        if (map.getView().getZoom() > 10) {
            map.getView().setZoom(10);
        }
    }
}

function formatValue(value, decimals, suffix) {
    if (value === null || value === undefined) {
        return '--';
    }
    let formatted = parseFloat(value).toFixed(decimals);
    return suffix ? formatted + ' ' + suffix : formatted;
}

function renderClimateSummaryTable(data) {
    let summaryData = data['climate_summary_data'];
    let table = $('#climate-summary-table');
    let currentUrl = window.location.href;
    let queryString = currentUrl.indexOf('?') !== -1 ? currentUrl.split('?')[1] : '';
    // Remove any existing siteId from the query so we can substitute per-site
    let baseQuery = queryString.replace(/siteId=[^&]*/g, '');
    if (baseQuery && !baseQuery.startsWith('&')) {
        baseQuery = '&' + baseQuery;
    }

    $.each(summaryData['site_code'], function (index, siteCode) {
        let siteId = summaryData['site_id'][index];
        let singleSiteUrl = '/climate/' + siteId + '/?siteId=' + siteId + baseQuery;

        let $tr = $('<tr>');
        if (summaryData['park_name']) {
            $tr.append('<td>' + (summaryData['park_name'][index] || '-') + '</td>');
        }
        $tr.append('<td>' + (summaryData['station_name'][index] || '-') + '</td>');
        $tr.append(
            '<td><a href="' + singleSiteUrl + '">' + siteCode + '</a></td>'
        );
        $tr.append('<td>' + formatValue(summaryData['avg_temp'][index], 1) + '</td>');
        $tr.append('<td>' + formatValue(summaryData['min_temp'][index], 1) + '</td>');
        $tr.append('<td>' + formatValue(summaryData['max_temp'][index], 1) + '</td>');
        $tr.append('<td>' + formatValue(summaryData['avg_humidity'][index], 1) + '</td>');
        $tr.append('<td>' + formatValue(summaryData['avg_windspeed'][index], 2) + '</td>');
        $tr.append('<td>' + formatValue(summaryData['total_rainfall'][index], 2) + '</td>');
        $tr.append('<td>' + formatValue(summaryData['max_rainfall'][index], 2) + '</td>');
        $tr.append('<td>' + (summaryData['record_count'][index] || '--') + '</td>');

        table.append($tr);
    });
}


function buildSummaryCSVBlob() {
    let headers = [];
    $('#climate-summary-table').closest('table').find('thead th').each(function () {
        headers.push($(this).text().trim());
    });

    let rows = [headers];
    $('#climate-summary-table tr').each(function () {
        let row = [];
        $(this).find('td').each(function () {
            row.push($(this).text().trim());
        });
        if (row.length) {
            rows.push(row);
        }
    });

    let csvContent = rows.map(function (row) {
        return row.map(function (cell) {
            return '"' + cell.replace(/"/g, '""') + '"';
        }).join(',');
    }).join('\r\n');

    return new Blob([csvContent], {type: 'text/csv;charset=utf-8;'});
}

function onDownloadSummaryCSVClicked(e) {
    let csvName = 'Climate-Summary';
    showDownloadPopup('CSV', csvName, function (downloadRequestId) {
        let blob = buildSummaryCSVBlob();
        uploadToDownloadRequest(downloadRequestId, blob, csvName + '.csv');
    });
}

function onDownloadMapClicked() {
    map.once('postrender', function () {
        let canvas = $('#map');
        html2canvas(canvas, {
            useCORS: true,
            background: '#FFFFFF',
            allowTaint: false,
            onrendered: function (canvas) {
                let link = document.createElement('a');
                link.setAttribute('type', 'hidden');
                link.href = canvas.toDataURL('image/png');
                link.download = 'climate_map.png';
                document.body.appendChild(link);
                link.click();
                link.remove();
            }
        });
    });
    map.renderSync();
}

function fetchData() {
    let params = window.location.href.split('dashboard-multi-sites')[1];
    let url = '/climate/dashboard-multi-sites-api' + params;
    $.get({
        url: url,
        dataType: 'json',
        success: function (data) {
            if (data.hasOwnProperty('status') && data['status'] === 'processing') {
                setTimeout(fetchData, 500);
                return;
            }
            $('.ajax-container').show();
            renderFilterList($('.filter-table'));
            drawMap(data);
            renderClimateSummaryTable(data);
        }
    });
}

$(function () {
    fetchData();

    Pace.on('hide', function () {
        $('.loading-title').hide();
    });

    $('.download-summary-as-csv').click(onDownloadSummaryCSVClicked);
    $('.download-map').click(onDownloadMapClicked);
    $('[data-toggle="tooltip"]').tooltip();
});
