(function () {
        const ORIGIN_DISPLAY_MAP = {
            'alien':'Non-Native',
            'indigenous': 'Native',
            'unknown': 'Unknown',
            'alien-invasive': 'Non-native: invasive',
            'alien-non-invasive': 'Non-native: non-invasive'
        }
        const queryString = window.location.search || '';
        if (typeof renderFilterList !== 'undefined') {
            renderFilterList($('.filter-history-table'));
        }

        function setSectionState(section, state) {
            const loading = section.querySelector('[data-loading]');
            const error = section.querySelector('[data-error]');
            const body = section.querySelector('[data-body]');
            if (state === 'loading') {
                loading.classList.remove('d-none');
                error.classList.add('d-none');
                body.classList.add('d-none');
            } else if (state === 'error') {
                loading.classList.add('d-none');
                error.classList.remove('d-none');
                body.classList.add('d-none');
            } else if (state === 'ready') {
                loading.classList.add('d-none');
                error.classList.add('d-none');
                body.classList.remove('d-none');
            }
        }

        function fetchWithPoll(url, onDone, onError) {
            fetch(url, {credentials: 'same-origin'})
                .then((response) => response.json())
                .then((data) => {
                    if (data && data.status && !['finished', 'FINISHED', 'SUCCESS'].includes(data.status)) {
                        setTimeout(() => fetchWithPoll(url, onDone, onError), 2000);
                        return;
                    }
                    onDone(data || {});
                })
                .catch(onError);
        }

        function downloadCanvasAsPng(canvasEl, title, downloadRequestId) {
            if (!canvasEl) {
                return;
            }
            canvasEl.toBlob(function (blob) {
                uploadToDownloadRequest(downloadRequestId, blob, title + '.png');
            }, 'image/png');
        }

        function renderStackedBarChart(responseData, chartCanvas, labelsDisplayMap = {}) {
            const labels = responseData.labels || [];
            const datasetLabels = responseData.dataset_labels || [];
            const colours = responseData.colours || {};
            const data = responseData.data || {};
            const palette = chartColors || defaultChartColors;
            const datasets = datasetLabels.map(function (label, idx) {
                return {
                    label: labelsDisplayMap.hasOwnProperty(label) ? labelsDisplayMap[label] : label,
                    backgroundColor: colours[label] || palette[idx % palette.length],
                    data: data[label] || []
                };
            });
            if (typeof Chart !== 'undefined') {
                if (chartCanvas._chartInstance) {
                    chartCanvas._chartInstance.destroy();
                }
                chartCanvas._chartInstance = new Chart(chartCanvas.getContext('2d'), {
                    type: 'bar',
                    data: {
                        labels: labels,
                        datasets: datasets
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        legend: {position: 'bottom'},
                        scales: {
                            xAxes: [{stacked: true}],
                            yAxes: [{
                                stacked: true,
                                ticks: {beginAtZero: true}
                            }]
                        }
                    }
                });
            }
        }

        function applyIucnNames(responseData, iucnNames) {
            if (!responseData || !iucnNames) {
                return responseData;
            }
            const updated = Object.assign({}, responseData);
            if (Array.isArray(updated.dataset_labels)) {
                updated.dataset_labels = updated.dataset_labels.map(function (label) {
                    return iucnNames[label] || label;
                });
            }
            if (updated.data) {
                const remapped = {};
                Object.keys(updated.data).forEach(function (key) {
                    remapped[iucnNames[key] || key] = updated.data[key];
                });
                updated.data = remapped;
            }
            if (updated.colours) {
                const remappedColours = {};
                Object.keys(updated.colours).forEach(function (key) {
                    remappedColours[iucnNames[key] || key] = updated.colours[key];
                });
                updated.colours = remappedColours;
            }
            return updated;
        }

        const defaultChartColors = [
            '#8D2641', '#641f30',
            '#E6E188', '#D7CD47',
            '#9D9739', '#525351',
            '#618295', '#2C495A',
            '#39B2A3', '#17766B',
            '#859FAC', '#1E2F38'
        ];
        let chartColors = null;
        function fetchChartColors(callback) {
            if (chartColors) {
                callback(chartColors);
                return;
            }
            $.ajax({
                url: '/api/chart-colors/',
                method: 'GET',
                success: function (data) {
                    chartColors = data;
                    callback(chartColors);
                },
                error: function () {
                    chartColors = defaultChartColors;
                    callback(chartColors);
                }
            });
        }

        function renderPieChart(chartData, canvasEl, legendEl, colorsOverride) {
            if (!chartData || !chartData.keys || chartData.keys.length === 0) {
                if (canvasEl && canvasEl.parentNode) {
                    canvasEl.parentNode.innerHTML = '<div class="spatial-dashboard-placeholder">No data</div>';
                }
                return;
            }
            if (canvasEl) {
                canvasEl.width = 200;
                canvasEl.height = 180;
            }
            const colors = colorsOverride || chartColors || defaultChartColors;
            const config = {
                type: 'pie',
                data: {
                    datasets: [{
                        data: chartData.data,
                        backgroundColor: colors
                    }],
                    labels: chartData.keys
                },
                options: {
                    responsive: false,
                    maintainAspectRatio: false,
                    legend: {display: false},
                    title: {display: false},
                    hover: {mode: 'nearest', intersect: false},
                    borderWidth: 0
                }
            };
            const ctx = canvasEl.getContext('2d');
            if (canvasEl._chartInstance) {
                canvasEl._chartInstance.destroy();
            }
            canvasEl._chartInstance = new Chart(ctx, config);
            if (legendEl) {
                const labels = chartData.keys.map(function (label, idx) {
                    const color = colors[idx] || '#999';
                    return '<div><span style="color:' + color + ';">■</span> ' + label + '</div>';
                });
                legendEl.innerHTML = labels.join('');
            }
        }

        const mapSection = document.getElementById('distribution-map-section');
        const mapError = mapSection.querySelector('[data-error]');
        const mapTarget = document.getElementById('spatial-dashboard-map');
        let map = null;
        let siteLayerSource = null;

        function buildBaseLayer() {
            let baseLayer = new ol.layer.Tile({
                source: new ol.source.OSM()
            });
            if (Array.isArray(baseMapLayers) && baseMapLayers.length > 0) {
                const reversed = baseMapLayers.slice().reverse();
                reversed.forEach(function (baseMapData) {
                    if (!baseMapData.default_basemap) {
                        return;
                    }
                    if (baseMapData.source_type === 'xyz') {
                        baseLayer = new ol.layer.Tile({
                            title: baseMapData.title,
                            source: new ol.source.XYZ({
                                attributions: [baseMapData.attributions],
                                url: '/bims_proxy/' + baseMapData.url
                            })
                        });
                    } else if (baseMapData.source_type === 'bing') {
                        baseLayer = new ol.layer.Tile({
                            title: baseMapData.title,
                            source: new ol.source.BingMaps({
                                key: baseMapData.key,
                                imagerySet: 'AerialWithLabels'
                            })
                        });
                    } else if (baseMapData.source_type === 'stamen') {
                        baseLayer = new ol.layer.Tile({
                            title: baseMapData.title,
                            source: new ol.source.XYZ({
                                attributions: [
                                    '&copy; <a href="https://www.stadiamaps.com/" target="_blank">Stadia Maps</a>',
                                    '&copy; <a href="https://stamen.com/" target="_blank">Stamen Design</a>',
                                    '&copy; <a href="https://openmaptiles.org/" target="_blank">OpenMapTiles</a>',
                                    '&copy; <a href="https://www.openstreetmap.org/about/" target="_blank">OpenStreetMap contributors</a>'
                                ],
                                url: '/bims_proxy/' + 'https://tiles-eu.stadiamaps.com/tiles/' + baseMapData.layer_name + '/{z}/{x}/{y}.jpg?api_key=' + baseMapData.key,
                                tilePixelRatio: 2,
                                maxZoom: 20
                            })
                        });
                    }
                });
            }
            return baseLayer;
        }

        function initMap() {
            if (map) {
                return;
            }
            const baseLayer = buildBaseLayer();
            siteLayerSource = new ol.source.ImageWMS({
                url: geoserverPublicUrl + 'wms',
                params: {
                    LAYERS: locationSiteGeoserverLayer,
                    FORMAT: 'image/png8',
                    viewparams: 'where:' + emptyWMSSiteParameter
                },
                ratio: 1,
                serverType: 'geoserver'
            });
            const siteLayer = new ol.layer.Image({
                source: siteLayerSource
            });
            map = new ol.Map({
                controls: ol.control.defaults.defaults().extend([
                    new ol.control.ScaleLine()
                ]),
                layers: [baseLayer, siteLayer],
                target: mapTarget,
                view: new ol.View({
                    center: [0, 0],
                    zoom: 2
                })
            });
            const graticule = new ol.layer.Graticule({
                strokeStyle: new ol.style.Stroke({
                    color: 'rgba(0,0,0,1)',
                    width: 1,
                    lineDash: [2.5, 4]
                }),
                showLabels: true
            });
            graticule.setMap(map);
        }

        setSectionState(mapSection, 'loading');
        initMap();
        fetchWithPoll('/api/spatial-dashboard/map/' + queryString, function (data) {
            const extent = data && data.extent ? data.extent : [];
            const viewName = data && data.sites_raw_query ? data.sites_raw_query : null;
            if (siteLayerSource) {
                const viewParam = viewName ? ('where:' + tenant + '."' + viewName + '"') : ('where:' + emptyWMSSiteParameter);
                siteLayerSource.updateParams({
                    viewparams: viewParam
                });
                siteLayerSource.refresh();
            }
            setSectionState(mapSection, 'ready');
            if (Array.isArray(extent) && extent.length === 4 && map) {
                const ext = ol.proj.transformExtent(
                    extent,
                    ol.proj.get('EPSG:4326'),
                    ol.proj.get('EPSG:3857')
                );
                setTimeout(function () {
                    map.updateSize();
                    map.getView().fit(ext, map.getSize());
                    if (map.getView().getZoom() > 8) {
                        map.getView().setZoom(8);
                    }
                }, 0);
            }
        }, function () {
            mapError.textContent = 'Failed to load map.';
            setSectionState(mapSection, 'error');
        });

        const summarySection = document.getElementById('summary-section');
        const summaryTable = document.getElementById('summary-table');
        const summaryError = summarySection.querySelector('[data-error]');
        let summaryModules = [];
        let summaryOrigin = {};
        let summaryEndemism = {};
        let summaryConsGlobal = {};
        let summaryConsNational = {};
        setSectionState(summarySection, 'loading');
        fetchWithPoll('/api/spatial-dashboard/summary/' + queryString, function (data) {
            const modules = data && data.modules ? data.modules : [];
            const overview = data && data.overview ? data.overview : {};
            const origin = data && data.origin ? data.origin : {};
            const endemism = data && data.endemism ? data.endemism : {};
            const consGlobal = data && data.cons_status_global ? data.cons_status_global : {};
            const consNational = data && data.cons_status_national ? data.cons_status_national : {};
            summaryModules = modules;
            summaryOrigin = origin;
            summaryEndemism = endemism;
            summaryConsGlobal = consGlobal;
            summaryConsNational = consNational;

            if (modules.length === 0) {
                summaryTable.innerHTML = '<tr><td>No results found.</td></tr>';
                setSectionState(summarySection, 'ready');
                return;
            }

            function renderHeaderRow() {
                const headerCells = ['<th></th>'];
                headerCells.push('<th colspan="' + modules.length + '">Number of taxa</th>');
                return '<tr>' + headerCells.join('') + '</tr>';
            }

            function renderModuleHeaderRow() {
                const headerCells = ['<th></th>'];
                modules.forEach(function (moduleName) {
                    headerCells.push('<th>' + moduleName + '</th>');
                });
                return '<tr>' + headerCells.join('') + '</tr>';
            }

            function renderSectionTitle(title) {
                return '<tr class="table-active"><th colspan="' + (modules.length + 1) + '">' + title + '</th></tr>';
            }

            function renderRows(rows, labelDisplayMap = {}) {
                const html = [];
                Object.keys(rows).forEach(function (label) {
                    const values = rows[label] || {}
                    if (labelDisplayMap.hasOwnProperty(label)) {
                        label = labelDisplayMap[label];
                    }
                    const cells = ['<td>' + label + '</td>'];
                    modules.forEach(function (moduleName) {
                        cells.push('<td>' + (values[moduleName] || '-') + '</td>');
                    });
                    html.push('<tr>' + cells.join('') + '</tr>');
                });
                return html.join('');
            }

            let html = '';
            html += renderHeaderRow();
            html += renderModuleHeaderRow();
            html += renderSectionTitle('Origin');
            html += renderRows(origin, ORIGIN_DISPLAY_MAP);
            html += renderSectionTitle('Endemism');
            html += renderRows(endemism);
            html += renderSectionTitle('Conservation status global');
            html += renderRows(consGlobal);
            html += renderSectionTitle('Conservation status national');
            html += renderRows(consNational);

            summaryTable.innerHTML = html;
            setSectionState(summarySection, 'ready');
        }, function () {
            summaryError.textContent = 'Failed to load summary.';
            setSectionState(summarySection, 'error');
        });

        const occurrencesSection = document.getElementById('occurrences-section');
        const occurrencesChartEl = document.getElementById('occurrences-chart');
        const occurrencesError = occurrencesSection.querySelector('[data-error]');
        setSectionState(occurrencesSection, 'loading');
        fetchWithPoll('/api/location-sites-total-occurrences-chart-data/' + queryString, function (data) {
            if (!data || !data.labels || data.labels.length === 0) {
                occurrencesChartEl.parentNode.innerHTML = '<div class="spatial-dashboard-placeholder">No results found.</div>';
                setSectionState(occurrencesSection, 'ready');
                return;
            }
            fetchChartColors(function () {
                renderStackedBarChart(data, occurrencesChartEl);
                setSectionState(occurrencesSection, 'ready');
            });
        }, function () {
            occurrencesError.textContent = 'Failed to load occurrences.';
            setSectionState(occurrencesSection, 'error');
        });

        const occurrencePiesSection = document.getElementById('occurrence-pies-section');
        const occurrencePiesError = occurrencePiesSection.querySelector('[data-error]');
        setSectionState(occurrencePiesSection, 'loading');
        fetchChartColors(function () {
            fetchWithPoll('/api/location-sites-summary/' + queryString, function (data) {
                const bio = data && data.biodiversity_data ? data.biodiversity_data : null;
                if (!bio || !bio.species) {
                    occurrencePiesSection.querySelector('[data-body]').innerHTML = '<div class="spatial-dashboard-placeholder">No results found.</div>';
                    setSectionState(occurrencePiesSection, 'ready');
                    return;
                }
                const species = bio.species;
                const originChart = species.origin_chart || {};
                const endemismChart = species.endemism_chart || {};
                const consChart = species.cons_status_chart || {};
                const samplingChart = species.sampling_method_chart || {};
                const biotopeChart = species.biotope_chart || {};
                const consNationalChart = species.cons_status_national_chart || null;

                const iucnNames = data.iucn_name_list || {};
                if (consChart.keys) {
                    consChart.keys = consChart.keys.map(function (k) {
                        return iucnNames[k] || k;
                    });
                }
                if (consNationalChart && consNationalChart.keys) {
                    consNationalChart.keys = consNationalChart.keys.map(function (k) {
                        return iucnNames[k] || k;
                    });
                }
                const originNames = data.origin_name_list || {};
                if (originChart.keys) {
                    const merged = {};
                    originChart.keys.forEach(function (k, idx) {
                        const label = originNames[k] !== undefined ? originNames[k] : (k || 'Unknown');
                        merged[label] = (merged[label] || 0) + (originChart.data[idx] || 0);
                    });
                    originChart.keys = Object.keys(merged);
                    originChart.data = Object.values(merged);
                }

                renderPieChart(originChart, document.getElementById('occurrence-origin-pie'),
                    document.getElementById('occurrence-origin-legend'), originChart.colours || chartColors);
                renderPieChart(endemismChart, document.getElementById('occurrence-endemism-pie'),
                    document.getElementById('occurrence-endemism-legend'), endemismChart.colours || chartColors);
                renderPieChart(consChart, document.getElementById('occurrence-cons-status-pie'),
                    document.getElementById('occurrence-cons-status-legend'), consChart.colours || chartColors);
                renderPieChart(samplingChart, document.getElementById('occurrence-sampling-pie'),
                    document.getElementById('occurrence-sampling-legend'), samplingChart.colours || chartColors);
                renderPieChart(biotopeChart, document.getElementById('occurrence-biotope-pie'),
                    document.getElementById('occurrence-biotope-legend'), biotopeChart.colours || chartColors);

                if (consNationalChart && consNationalChart.keys && consNationalChart.keys.length > 0) {
                    renderPieChart(consNationalChart, document.getElementById('occurrence-cons-status-national-pie'),
                        document.getElementById('occurrence-cons-status-national-legend'), consNationalChart.colours || chartColors);
                } else {
                    const wrapper = document.getElementById('occurrence-cons-status-national-wrapper');
                    if (wrapper) {
                        wrapper.style.display = 'none';
                        wrapper.parentNode.style.display = 'none'
                    }
                }

                setSectionState(occurrencePiesSection, 'ready');
            }, function () {
                occurrencePiesError.textContent = 'Failed to load occurrence breakdown.';
                setSectionState(occurrencePiesSection, 'error');
            });
        });

        const originSection = document.getElementById('origin-section');
        const originChartEl = document.getElementById('origin-chart');
        const originError = originSection.querySelector('[data-error]');
        setSectionState(originSection, 'loading');
        fetchWithPoll('/api/location-sites-occurrences-chart-data/' + queryString, function (data) {
            if (!data || !data.labels || data.labels.length === 0) {
                originChartEl.parentNode.innerHTML = '<div class="spatial-dashboard-placeholder">No results found.</div>';
                setSectionState(originSection, 'ready');
                return;
            }
            fetchChartColors(function () {
                renderStackedBarChart(data, originChartEl, ORIGIN_DISPLAY_MAP);
                setSectionState(originSection, 'ready');
            });
        }, function () {
            originError.textContent = 'Failed to load origin.';
            setSectionState(originSection, 'error');
        });

        const endemismSection = document.getElementById('endemism-section');
        const endemismChartEl = document.getElementById('endemism-chart');
        const endemismError = endemismSection.querySelector('[data-error]');
        setSectionState(endemismSection, 'loading');
        fetchWithPoll('/api/location-sites-endemism-chart-data/' + queryString, function (data) {
            if (!data || !data.labels || data.labels.length === 0) {
                endemismChartEl.parentNode.innerHTML = '<div class="spatial-dashboard-placeholder">No results found.</div>';
                setSectionState(endemismSection, 'ready');
                return;
            }
            fetchChartColors(function () {
                renderStackedBarChart(data, endemismChartEl);
                setSectionState(endemismSection, 'ready');
            });
        }, function () {
            endemismError.textContent = 'Failed to load endemism.';
            setSectionState(endemismSection, 'error');
        });

        const consSection = document.getElementById('cons-status-section');
        const consChartEl = document.getElementById('cons-status-chart');
        const consError = consSection.querySelector('[data-error]');
        setSectionState(consSection, 'loading');
        fetchWithPoll('/api/location-sites-cons-chart-data/' + queryString, function (data) {
            if (!data || !data.labels || data.labels.length === 0) {
                consChartEl.parentNode.innerHTML = '<div class="spatial-dashboard-placeholder">No results found.</div>';
                setSectionState(consSection, 'ready');
                return;
            }
            fetchWithPoll('/api/location-sites-summary/' + queryString, function (summaryData) {
                const iucnNames = summaryData && summaryData.iucn_name_list ? summaryData.iucn_name_list : {};
                const updatedData = applyIucnNames(data, iucnNames);
                fetchChartColors(function () {
                    renderStackedBarChart(updatedData, consChartEl);
                    setSectionState(consSection, 'ready');
                });
            }, function () {
                fetchChartColors(function () {
                    renderStackedBarChart(data, consChartEl);
                    setSectionState(consSection, 'ready');
                });
            });
        }, function () {
            consError.textContent = 'Failed to load conservation status.';
            setSectionState(consSection, 'error');
        });

        const consPerModuleSection = document.getElementById('cons-status-per-module-section');
        const consChartPerModuleEl = document.getElementById('cons-status-chart-per-module');
        const consErrorPerModule = consSection.querySelector('[data-error]');
        var consPerModuleData = null;
        setSectionState(consPerModuleSection, 'loading');
        fetchWithPoll('/api/spatial-dashboard/cons-status/' + queryString, function (data) {
            const modules = (data && data.modules) ? data.modules : [];
            if (modules.length === 0) {
                consChartPerModuleEl.parentNode.innerHTML = '<div class="spatial-dashboard-placeholder">No results found.</div>';
                setSectionState(consPerModuleSection, 'ready');
                return;
            }

            // Order from worst (highest extinction risk) to best (least risk)
            var CATEGORY_ORDER = [
                'EX', 'RE', 'EW', 'CR PE', 'CR', 'EN', 'VU',
                'D', 'NT', 'LR/cd', 'LR/nt', 'CA', 'RA', 'LC', 'LR/lc'
            ];

            const filteredModules = modules.filter(function (module) {
                return (module.cons_status || []).reduce(function (sum, item) {
                    return sum + (item.count || 0);
                }, 0) > 0;
            });
            if (filteredModules.length === 0) {
                consChartPerModuleEl.parentNode.innerHTML = '<div class="spatial-dashboard-placeholder">No results found.</div>';
                setSectionState(consPerModuleSection, 'ready');
                return;
            }

            var categoryMap = {};
            filteredModules.forEach(function (module) {
                (module.cons_status || []).forEach(function (item) {
                    var key = item.name || item.category || 'Unknown';
                    if (!categoryMap[key]) {
                        categoryMap[key] = {
                            colour: item.colour || '#999',
                            code: item.category || ''
                        };
                    }
                });
            });
            var categories = Object.keys(categoryMap);
            categories.sort(function (a, b) {
                var idxA = CATEGORY_ORDER.indexOf(categoryMap[a].code);
                var idxB = CATEGORY_ORDER.indexOf(categoryMap[b].code);
                if (idxA === -1) idxA = CATEGORY_ORDER.length;
                if (idxB === -1) idxB = CATEGORY_ORDER.length;
                return idxA - idxB;
            });
            const moduleLabels = filteredModules.map(function (module) { return module.name; });
            const totals = filteredModules.map(function (module) {
                return (module.cons_status || []).reduce(function (sum, item) {
                    return sum + (item.count || 0);
                    }, 0);
            });
            var datasets = categories.map(function (category) {
                return {
                    label: category,
                    backgroundColor: categoryMap[category].colour,
                    borderColor: '#555555',
                    borderWidth: 1,
                    data: filteredModules.map(function (module, idx) {
                        const total = totals[idx] || 0;
                        const match = (module.cons_status || []).find(function (item) {
                            return (item.name || item.category || 'Unknown') === category;
                        });
                        const count = match ? (match.count || 0) : 0;
                        return Math.round((count / total) * 1000) / 10;
                    })
                };
            });
            consPerModuleData = {
                moduleLabels: moduleLabels,
                categories: categories,
                datasets: datasets
            };
            if (typeof Chart !== 'undefined') {
                var barHeight = 30;
                var chartHeight = Math.max(160, moduleLabels.length * (barHeight + 10) + 60);
                consChartPerModuleEl.parentNode.style.height = chartHeight + 'px';
                consChartPerModuleEl.height = chartHeight;
                new Chart(consChartPerModuleEl.getContext('2d'), {
                    type: 'horizontalBar',
                    data: {
                        labels: moduleLabels,
                        datasets: datasets
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        legend: {position: 'bottom'},
                        scales: {
                            xAxes: [{
                                stacked: true,
                                ticks: {
                                    beginAtZero: true,
                                    max: 100,
                                    callback: function (value) { return value + '%'; }
                                }
                            }],
                            yAxes: [{
                                stacked: true,
                                barThickness: barHeight,
                                maxBarThickness: barHeight
                            }]
                        },
                        tooltips: {
                            callbacks: {
                                label: function (tooltipItem, chartData) {
                                    const dataset = chartData.datasets[tooltipItem.datasetIndex];
                                    return dataset.label + ': ' + tooltipItem.xLabel + '%';
                                }
                            }
                        }
                    }
                });
            }
            setSectionState(consPerModuleSection, 'ready');
        }, function () {
            consErrorPerModule.textContent = 'Failed to load conservation status.';
            setSectionState(consPerModuleSection, 'error');
        });


        const rliSection = document.getElementById('rli-section');
        const rliChartEl = document.getElementById('rli-chart');
        const rliError = rliSection.querySelector('[data-error]');
        setSectionState(rliSection, 'loading');
        fetchWithPoll('/api/spatial-dashboard/rli/' + queryString, function (data) {
            const series = (data && data.series) ? data.series : [];
            const aggregate = (data && data.aggregate) ? data.aggregate : [];
            if (series.length === 0 && aggregate.length === 0) {
                rliChartEl.parentNode.innerHTML = '<div class="spatial-dashboard-placeholder">No results found.</div>';
                setSectionState(rliSection, 'ready');
                return;
            }

            const palette = [
                '#1976d2', '#388e3c', '#f57c00', '#7b1fa2', '#00796b',
                '#c2185b', '#5d4037', '#455a64', '#512da8', '#0097a7',
                '#689f38', '#fbc02d'
            ];

            const datasets = series.map(function (item, idx) {
                return {
                    label: item.name,
                    data: item.points.map(function (point) {
                        return {x: point.year, y: point.value};
                    }),
                    borderColor: palette[idx % palette.length],
                    backgroundColor: 'transparent',
                    fill: false,
                    lineTension: 0,
                    pointRadius: 3
                };
            });

            if (aggregate.length > 0) {
                datasets.unshift({
                    label: 'Aggregate',
                    data: aggregate.map(function (point) {
                        return {x: point.year, y: point.value};
                    }),
                    borderColor: '#000',
                    backgroundColor: 'transparent',
                    fill: false,
                    borderDash: [6, 4],
                    lineTension: 0,
                    pointRadius: 2
                });
            }

            if (typeof Chart !== 'undefined') {
                if (rliChartEl._chartInstance) {
                    rliChartEl._chartInstance.destroy();
                }
                rliChartEl._chartInstance = new Chart(rliChartEl.getContext('2d'), {
                    type: 'line',
                    data: {datasets: datasets},
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        legend: {position: 'right'},
                        scales: {
                            xAxes: [{
                                type: 'linear',
                                position: 'bottom',
                                ticks: {
                                    callback: function (value) { return value; }
                                },
                                scaleLabel: {display: true, labelString: 'Year'}
                            }],
                            yAxes: [{
                                ticks: {
                                    min: 0,
                                    max: 1,
                                    callback: function (value) { return value.toFixed(2); }
                                },
                                scaleLabel: {display: true, labelString: 'Red List Index'}
                            }]
                        },
                        tooltips: {
                            callbacks: {
                                label: function (tooltipItem, chartData) {
                                    const dataset = chartData.datasets[tooltipItem.datasetIndex];
                                    return dataset.label + ': ' + tooltipItem.yLabel.toFixed(3);
                                }
                            }
                        }
                    }
                });
            }
            setSectionState(rliSection, 'ready');
        }, function () {
            rliError.textContent = 'Failed to load red list index.';
            setSectionState(rliSection, 'error');
        });

        const nationalConsSection = document.getElementById('national-cons-status-section');
        if (nationalConsSection) {
            const nationalConsChartEl = document.getElementById('national-cons-status-chart');
            setSectionState(nationalConsSection, 'loading');
            fetchWithPoll('/api/spatial-dashboard/national-cons-status/' + queryString, function (data) {
                const series = (data && data.series) ? data.series : [];
                const aggregate = (data && data.aggregate) ? data.aggregate : [];
                if (series.length === 0 && aggregate.length === 0) {
                    nationalConsChartEl.parentNode.innerHTML = '<div class="spatial-dashboard-placeholder">No results found.</div>';
                    setSectionState(nationalConsSection, 'ready');
                    return;
                }

                const xLabels = aggregate.map(function (p) { return p.label; });

                const palette = [
                    '#1976d2', '#388e3c', '#f57c00', '#7b1fa2', '#00796b',
                    '#c2185b', '#5d4037', '#455a64', '#512da8', '#0097a7',
                    '#689f38', '#fbc02d'
                ];

                const datasets = series.map(function (item, idx) {
                    return {
                        label: item.name,
                        data: xLabels.map(function (lbl) {
                            const pt = item.points.find(function (p) { return p.label === lbl; });
                            return pt ? pt.value : null;
                        }),
                        borderColor: palette[idx % palette.length],
                        backgroundColor: 'transparent',
                        fill: false,
                        lineTension: 0,
                        pointRadius: 4,
                        spanGaps: false,
                    };
                });

                if (aggregate.length > 0) {
                    datasets.unshift({
                        label: 'Aggregate',
                        data: aggregate.map(function (p) { return p.value; }),
                        borderColor: '#000',
                        backgroundColor: 'transparent',
                        fill: false,
                        borderDash: [6, 4],
                        lineTension: 0,
                        pointRadius: 3,
                    });
                }

                if (typeof Chart !== 'undefined') {
                    if (nationalConsChartEl._chartInstance) {
                        nationalConsChartEl._chartInstance.destroy();
                    }
                    nationalConsChartEl._chartInstance = new Chart(nationalConsChartEl.getContext('2d'), {
                        type: 'line',
                        data: {labels: xLabels, datasets: datasets},
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            legend: {position: 'right'},
                            scales: {
                                xAxes: [{
                                    scaleLabel: {display: true, labelString: 'Assessment'},
                                }],
                                yAxes: [{
                                    ticks: {
                                        min: 0,
                                        max: 1,
                                        callback: function (value) { return value.toFixed(2); }
                                    },
                                    scaleLabel: {display: true, labelString: 'Red List Index'}
                                }]
                            },
                            tooltips: {
                                callbacks: {
                                    label: function (tooltipItem, chartData) {
                                        const dataset = chartData.datasets[tooltipItem.datasetIndex];
                                        return dataset.label + ': ' + parseFloat(tooltipItem.yLabel).toFixed(3);
                                    }
                                }
                            }
                        }
                    });
                }

                setSectionState(nationalConsSection, 'ready');
            }, function () {
                setSectionState(nationalConsSection, 'error');
            });
        }

        var speciesDownloadBtn = document.getElementById('species-download-btn');
        var speciesDownloadStatus = document.getElementById('species-download-status');
        if (speciesDownloadBtn) {
            speciesDownloadBtn.addEventListener('click', function () {
                showDownloadPopup('CSV', 'Species List', function (downloadRequestId) {
                    speciesDownloadBtn.disabled = true;
                    speciesDownloadStatus.textContent = '';

                    $('#alertModalBody').html(downloadRequestMessage);
                    $('#alertModal').modal({'keyboard': false, 'backdrop': 'static'});

                    var sep = queryString ? '&' : '?';
                    var downloadUrl = '/api/spatial-dashboard/species-download/' + queryString +
                        sep + 'downloadRequestId=' + (downloadRequestId || '');

                    fetchWithPoll(downloadUrl,
                        function () { speciesDownloadBtn.disabled = false; },
                        function () { speciesDownloadBtn.disabled = false; }
                    );
                }, false, null, false);
            });
        }

        document.querySelectorAll('[data-download]').forEach(function (button) {
            button.addEventListener('click', function () {
                const type = button.getAttribute('data-download');
                if (type === 'map') {
                    if (!map) {
                        return;
                    }
                    map.once('postrender', function () {
                        showDownloadPopup('IMAGE', 'Distribution Map', function (downloadRequestId) {
                            const canvas = $('#spatial-dashboard-map')[0];
                            html2canvas(canvas, {
                                useCORS: true,
                                background: '#FFFFFF',
                                allowTaint: false,
                                onrendered: function (canvasEl) {
                                    canvasEl.toBlob(function (blob) {
                                        uploadToDownloadRequest(downloadRequestId, blob, 'Distribution Map.png');
                                    }, 'image/png');
                                }
                            });
                        });
                    });
                    map.renderSync();
                }
                if (type === 'cons-status') {
                    showDownloadPopup('CHART', 'Conservation Status', function (downloadRequestId) {
                        downloadCanvasAsPng(consChartEl, 'conservation-status', downloadRequestId);
                    });
                }
                if (type === 'occurrences') {
                    showDownloadPopup('CHART', 'Occurrences', function (downloadRequestId) {
                        downloadCanvasAsPng(occurrencesChartEl, 'occurrences', downloadRequestId);
                    });
                }
                if (type === 'origin') {
                    showDownloadPopup('CHART', 'Origin', function (downloadRequestId) {
                        downloadCanvasAsPng(originChartEl, 'origin', downloadRequestId);
                    });
                }
                if (type === 'endemism') {
                    showDownloadPopup('CHART', 'Endemism', function (downloadRequestId) {
                        downloadCanvasAsPng(endemismChartEl, 'endemism', downloadRequestId);
                    });
                }
                if (type === 'rli') {
                    showDownloadPopup('CHART', 'IUCN Red List Index of species (RLI)', function (downloadRequestId) {
                        downloadCanvasAsPng(rliChartEl, 'red-list-index', downloadRequestId);
                    });
                }
                if (type === 'national-cons-status') {
                    var nationalChartEl = document.getElementById('national-cons-status-chart');
                    if (nationalChartEl) {
                        showDownloadPopup('CHART', 'National Conservation Status', function (downloadRequestId) {
                            downloadCanvasAsPng(nationalChartEl, 'national-conservation-status', downloadRequestId);
                        });
                    }
                }
                if (type === 'cons-status-per-module') {
                    showDownloadPopup('CHART', 'Conservation Status Global (IUCN)', function (downloadRequestId) {
                        downloadCanvasAsPng(consChartPerModuleEl, 'conservation-status-per-module', downloadRequestId);
                    });
                }
                if (type === 'cons-status-per-module-csv') {
                    if (!consPerModuleData) {
                        return;
                    }
                    showDownloadPopup('CSV', 'Conservation Status Global (IUCN)', function () {
                        var moduleLabels = consPerModuleData.moduleLabels;
                        var categories = consPerModuleData.categories;
                        var datasets = consPerModuleData.datasets;

                        function escapeCell(cell) {
                            var s = String(cell);
                            return (s.indexOf(',') !== -1 || s.indexOf('"') !== -1 || s.indexOf('\n') !== -1)
                                ? '"' + s.replace(/"/g, '""') + '"' : s;
                        }

                        var rows = [];
                        rows.push(['Module'].concat(categories).map(escapeCell).join(','));
                        moduleLabels.forEach(function (moduleName, moduleIdx) {
                            var row = [moduleName];
                            categories.forEach(function (category, catIdx) {
                                var value = datasets[catIdx] ? (datasets[catIdx].data[moduleIdx] || 0) : 0;
                                row.push(value + '%');
                            });
                            rows.push(row.map(escapeCell).join(','));
                        });

                        var csvContent = rows.join('\n');
                        var blob = new Blob([csvContent], {type: 'text/csv;charset=utf-8;'});
                        var url = URL.createObjectURL(blob);
                        var link = document.createElement('a');
                        link.href = url;
                        link.download = 'conservation-status-per-module.csv';
                        document.body.appendChild(link);
                        link.click();
                        link.remove();
                        URL.revokeObjectURL(url);
                    }, false);
                }
                if (type === 'overview') {
                    showDownloadPopup('CSV', 'Overview', function () {
                        const rows = [];
                        const headerCols = [''];
                        summaryModules.forEach(function (m) { headerCols.push(m); });
                        rows.push(headerCols);

                        function addSectionRows(title, sectionData, labelDisplayMap) {
                            rows.push([title]);
                            Object.keys(sectionData).forEach(function (label) {
                                const values = sectionData[label] || {};
                                const displayLabel = labelDisplayMap && labelDisplayMap.hasOwnProperty(label) ? labelDisplayMap[label] : label;
                                const row = [displayLabel];
                                summaryModules.forEach(function (m) { row.push(values[m] || 0); });
                                rows.push(row);
                            });
                        }

                        addSectionRows('Origin', summaryOrigin, ORIGIN_DISPLAY_MAP);
                        addSectionRows('Endemism', summaryEndemism, {});
                        addSectionRows('Conservation status global', summaryConsGlobal, {});
                        addSectionRows('Conservation status national', summaryConsNational, {});

                        const csvContent = rows.map(function (row) {
                            return row.map(function (cell) {
                                const s = String(cell);
                                return (s.indexOf(',') !== -1 || s.indexOf('"') !== -1 || s.indexOf('\n') !== -1)
                                    ? '"' + s.replace(/"/g, '""') + '"' : s;
                            }).join(',');
                        }).join('\n');

                        const blob = new Blob([csvContent], {type: 'text/csv;charset=utf-8;'});
                        const url = URL.createObjectURL(blob);
                        const link = document.createElement('a');
                        link.href = url;
                        link.download = 'overview.csv';
                        document.body.appendChild(link);
                        link.click();
                        link.remove();
                        URL.revokeObjectURL(url);
                    }, false);
                }
            });
        });
        $('[data-toggle="tooltip"]').tooltip();
        $('[data-toggle="popover"]').popover({
            container: 'body',
            delay: { show: 100, hide: 150 }
        });
    })();