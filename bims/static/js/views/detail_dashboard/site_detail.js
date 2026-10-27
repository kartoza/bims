define([
    'backbone',
    'underscore',
    'jquery',
    'gridStack',
    'shared',
    'htmlToCanvas',
    'chartJs',
    'utils/filter_list',
    'views/ogc_live_layer',
], function (
    Backbone,
    _,
    $,
    gridStack,
    Shared,
    HtmlToCanvas,
    ChartJs,
    filterList,
    OgcLiveLayer
) {
    return Backbone.View.extend({
        id: 'detailed-site-dashboard',
        isOpen: false,
        template: _.template($('#detailed-site-dashboard').html()),
        dummyPieData: [25, 2, 7, 10, 12, 25, 60],
        objectDataByYear: 'object_data_by_year',
        yearsArray: 'years_array',
        dummyPieColors: ['#2d2d2d', '#565656', '#6d6d6d', '#939393', '#adadad', '#bfbfbf', '#d3d3d3'],
        fetchBaseUrl: '/api/location-sites-summary/?',
        fetchLocationSiteCoordinateUrl: '/api/location-sites-coordinate/?',
        csvDownloadEmailUrl: '/api/csv-download/',
        chemCsvDownloadUrl: '/api/chemical-record/download/',
        locationSiteCoordinateRequestXHR: null,
        apiParameters: _.template(Shared.SearchURLParametersTemplate),
        uniqueSites: [],
        occurrenceData: {},
        vectorLayerFromMainMap: null,
        siteLayerSource: null,
        siteLayerVector: null,
        siteId: null,
        currentDashboardWrapper: null,
        grid: null,
        currentModule: '',
        currentFiltersUrl: '',
        chartConfigs: [],
        chartColors: null,
        pieOptions: {
            legend: {
                display: true
            },
            cutoutPercentage: 0,
            maintainAspectRatio: true,
        },
        dashboardData: null,
        csvDownloadGbifIdsUrl: '/api/gbif-ids/download/',
        checklistDownloadUrl: '/api/checklist/download/',
        events: {
            'click .close-dashboard': 'closeDashboard',
            'click #export-locationsite-map': 'exportLocationsiteMap',
            'click .download-origin-chart': 'downloadOriginChart',
            'click .download-record-timeline': 'downloadRecordTimeline',
            'click .download-collection-timeline': 'downloadCollectionTimeline',
            'click .download-as-csv': 'downloadAsCSV',
            'click .download-chem-csv': 'downloadChemRecordsAsCSV',
            'click .ssdd-export': 'downloadElementEvent',
            'click .download-citation': 'downloadCitationList',
            'click .download-chart-image': 'downloadChartImage',
            'click #chem-graph-export': 'downloadChemGraphs',
            'click .btn-html-img': 'convertToPNG',
            'click .download-gbif-ids' : 'downloadGBIfIds',
            'click .download-checklist' : 'downloadChecklist',
            'click .publish-live-layer': 'publishLiveLayer',
            'change .dashboard-data-frequency': 'frequencyChanged'
        },
        initialize: function (options) {
            _.bindAll(this, 'render');
            this.parent = options.parent;
            this.$el.hide();
            this.mapLocationSite = null;
            this.fetchChartColors();
        },
        fetchChartColors: function () {
            var self = this;
            $.ajax({
                url: '/api/chart-colors/',
                method: 'GET',
                success: function (data) {
                    self.chartColors = data;
                },
                error: function () {
                    // Fallback to default colors if API fails
                    self.chartColors = [
                        '#8D2641', '#641f30',
                        '#E6E188', '#D7CD47',
                        '#9D9739', '#525351',
                        '#618295', '#2C495A',
                        '#39B2A3', '#17766B',
                        '#859FAC', '#1E2F38'
                    ];
                }
            });
        },
        render: function () {
            var self = this;
            this.$el.html(this.template());

            this.loadingDashboard = this.$el.find('.loading-dashboard');
            this.occurrenceTable = this.$el.find('#occurence-table');
            this.siteMarkers = this.$el.find('#site-markers');

            this.originTimelineGraph = this.$el.find('#collection-timeline-graph')[0];
            this.originCategoryGraph = this.$el.find('#collection-category-graph')[0];
            this.recordsTimelineGraph = this.$el.find('#records-timeline-graph')[0];

            this.siteName = this.$el.find('#site-name');
            this.siteNameWrapper = this.siteName.parent();
            this.siteNameWrapper.hide();
            this.totalRecords = this.$el.find('#total-records');

            this.iconStyle = new ol.style.Style({
                image: new ol.style.Icon(({
                    anchor: [0.5, 46],
                    anchorXUnits: 'fraction',
                    anchorYUnits: 'pixels',
                    opacity: 0.75,
                    src: '/static/img/map-marker.png'
                }))
            });

            let biodiversityLayersOptions = {
                url: geoserverPublicUrl + 'wms',
                params: {
                    LAYERS: locationSiteGeoserverLayer,
                    FORMAT: 'image/png8',
                    viewparams: 'where:' + emptyWMSSiteParameter
                },
                ratio: 1,
                serverType: 'geoserver'
            };
            this.siteLayerSource = new ol.source.ImageWMS(biodiversityLayersOptions);
            this.siteTileLayer = new ol.layer.Image({
                source: self.siteLayerSource
            });

            return this;
        },
        frequencyChanged: function (e) {
            let $target = $(e.target);
            let newParam = 'd=' + $target.val();
            let url = new URL(this.currentFiltersUrl, window.location.origin);
            let searchParams = new URLSearchParams(url.search);
            if (searchParams.has('d')) {
                searchParams.set('d', $target.val());
            } else {
                searchParams.append('d', $target.val());
            }
            url.search = searchParams.toString();
            this.currentFiltersUrl = url.search;
            this.loadingDashboard.show();
            this.fetchData(url.search.replace('?', ''));
        },
        renderMap: function (data, target = 'locationsite-map') {
            let self = this;
            const baseLayer = [];
            if (!self.mapLocationSite) {
                let _baseMap =  new ol.layer.Tile({
                    source: new ol.source.OSM({
                        tileLoadFunction: (imageTile, src) => {
                            const img = imageTile.getImage();
                            if (img && 'referrerPolicy' in img) {
                                img.referrerPolicy = 'origin-when-cross-origin';
                            }
                            img.src = src;
                        }
                    })
                });
                $.each(baseMapLayers.reverse(), function (index, baseMapData) {
                    if (baseMapData.default_basemap) {
                        if (baseMapData['source_type'] === "xyz") {
                            _baseMap = new ol.layer.Tile({
                                title: baseMapData['title'],
                                source: new ol.source.XYZ({
                                    attributions: [baseMapData['attributions']],
                                    url: '/bims_proxy/' + baseMapData['url']
                                })
                            })
                        } else if (baseMapData['source_type'] === "bing") {
                            _baseMap = new ol.layer.Tile({
                                title: baseMapData['title'],
                                source: new ol.source.BingMaps({
                                    key: baseMapData['key'],
                                    imagerySet: 'AerialWithLabels'
                                })
                            });
                        } else if (baseMapData['source_type'] === "stamen") {
                            _baseMap = new ol.layer.Tile({
                                title: baseMapData['title'],
                                source: new ol.source.XYZ({
                                    attributions: [
                                        '&copy; <a href="https://www.stadiamaps.com/" target="_blank">Stadia Maps</a>',
                                        '&copy; <a href="https://stamen.com/" target="_blank">Stamen Design</a>',
                                        '&copy; <a href="https://openmaptiles.org/" target="_blank">OpenMapTiles</a>',
                                        '&copy; <a href="https://www.openstreetmap.org/about/" target="_blank">OpenStreetMap contributors</a>'
                                    ],
                                    url: '/bims_proxy/' + 'https://tiles-eu.stadiamaps.com/tiles/' + baseMapData['layer_name'] + '/{z}/{x}/{y}.jpg?api_key=' + baseMapData['key'],
                                    tilePixelRatio: 2,
                                    maxZoom: 20
                                })
                            });
                        }
                    }
                })

                baseLayer.push(_baseMap)
                self.mapLocationSite = new ol.Map({
                    controls: ol.control.defaults.defaults().extend([
                        new ol.control.ScaleLine()
                    ]),
                    layers: baseLayer,
                    target: target,
                    view: new ol.View({
                        center: [0, 0],
                        zoom: 2
                    })
                });
                self.mapLocationSite.addLayer(self.siteTileLayer);
                let graticule = new ol.layer.Graticule({
                    strokeStyle: new ol.style.Stroke({
                        color: 'rgba(0,0,0,1)',
                        width: 1,
                        lineDash: [2.5, 4]
                    }),
                    showLabels: true
                });
                graticule.setMap(self.mapLocationSite);
            }
             // Zoom to extent
            if (data['extent'].length > 0) {
                let ext = ol.proj.transformExtent(
                    data['extent'],
                    ol.proj.get('EPSG:4326'),
                    ol.proj.get('EPSG:3857'));
                self.mapLocationSite.getView().fit(ext, self.mapLocationSite.getSize());
                if (self.mapLocationSite.getView().getZoom() > 8) {
                    self.mapLocationSite.getView().setZoom(8);
                }
            }
            let newParams = {
                layers: locationSiteGeoserverLayer,
                format: 'image/png',
                viewparams: 'where:' + tenant + '."' + data['sites_raw_query'] + '"'
            };
            self.siteLayerSource.updateParams(newParams);
            self.siteLayerSource.refresh();
        },
        show: function (data) {
            if (this.isOpen) {
                return false;
            }
            Shared.Dispatcher.trigger('bugReport:moveRight');
            var self = this;
            this.isOpen = true;
            this.$el.show('slide', {
                direction: 'right'
            }, 300, function () {
                if (typeof data === 'string') {
                    self.chemCsvDownloadUrl += '?' + data;
                    self.csvDownloadEmailUrl += '?' + data;
                    self.fetchData(data, true);
                    self.currentFiltersUrl = '?' + data;
                    self.csvDownloadGbifIdsUrl += '?' + data;
                    self.checklistDownloadUrl += '?' + data;
                } else {
                    self.chemCsvDownloadUrl += self.apiParameters(filterParameters);
                    self.csvDownloadEmailUrl += self.apiParameters(filterParameters);
                    self.currentFiltersUrl = self.apiParameters(filterParameters);
                    self.csvDownloadGbifIdsUrl += self.apiParameters(filterParameters);
                    self.checklistDownloadUrl += self.apiParameters(filterParameters)
                    self.fetchData(self.apiParameters(filterParameters).substr(1), false);
                    Shared.Router.updateUrl('site-detail/' + self.apiParameters(filterParameters).substr(1), true);
                }
            });
        },
        fetchData: function (parameters, multipleSites) {
            var self = this;

            if (Shared.LocationSiteDetailXHRRequest) {
                Shared.LocationSiteDetailXHRRequest.abort();
                Shared.LocationSiteDetailXHRRequest = null;
            }

            Shared.LocationSiteDetailXHRRequest = $.get({
                url: self.fetchBaseUrl + parameters,
                dataType: 'json',
                success: function (data) {
                    if (data.hasOwnProperty('status')) {
                        let status = data['status'].toLowerCase()
                        if (status === 'processing' || status === 'started' || status === 'progress') {
                            setTimeout(function () {
                                self.fetchData(parameters);
                            }, 1000);
                            return false;
                        }
                        if (status === 'failure') {
                            self.closeDashboard();
                            return false
                        }
                    }

                    if (typeof data['modules'] !== 'undefined') {
                        self.currentModule = data['modules'].join();
                    }
                    self.dashboardData = data;

                    // Check dashboard configuration
                    let $dashboardWrapper = self.$el.find('.dashboard-wrapper');
                    let dashboardHeader = self.$el.find('.dashboard-header');

                    if (data['is_multi_sites']) {
                        dashboardHeader.html('MULTIPLE SITES DASHBOARD - ' + self.currentModule.toUpperCase());
                    } else {
                        dashboardHeader.html('SINGLE SITE - ' + self.currentModule.toUpperCase());
                    }
                    // Remove default height for chart containers
                    $('#species-ssdd-occurrences-line-chart').css("height", "")
                    $('#species-ssdd-taxa-occurrences-bar-chart').css("height", "")
                    $('#species-ssdd-origin-bar-chart').css("height", "")
                    $('#species-ssdd-endemism-bar-chart').css("height", "")
                    $('#species-ssdd-cons-status-bar-chart').css("height", "")
                    $('#species-ssdd-occurrence-data').css("height", "")

                    if (Object.keys(data['dashboard_configuration']).length !== 0) {

                        let $moduleDashboardWrapper = $(
                            `<div class="container grid-stack dashboard-wrapper-${self.currentModule}" style="padding-right: 40px !important;"></div>`
                        );
                        self.currentDashbordWrapper = $moduleDashboardWrapper;
                        $dashboardWrapper.before($moduleDashboardWrapper);
                        $dashboardWrapper.hide();

                        // Render the layouts
                        self.renderCustomDashboardLayout($moduleDashboardWrapper, data['dashboard_configuration']);

                        $.each(data['dashboard_configuration'], (index, configuration) => {
                            let $container = $moduleDashboardWrapper.find(`[data-key='${configuration['key']}']`).find('.grid-stack-item-content');
                            // Render content for each container
                            switch(configuration['key']) {
                                case 'filter-history': {
                                    renderFilterList($container.find('.content-body'));
                                    break;
                                }
                                case 'overview': {
                                    if (data['is_multi_sites']) {
                                        self.createMultiSiteDetails(data, $container.find('.records-overview'));
                                    } else {
                                        self.renderSingleSiteDetails(data);
                                    }
                                    break;
                                }
                                case 'occurrences': {
                                    self.createOccurrencesBarChart(data, $container.find('.content-body')[0]);
                                    break;
                                }
                                case 'occurrences-data': {
                                    self.createOccurrenceDataTable(data, $container);
                                    break;
                                }
                                case 'distribution-map': {
                                    let map = $container.find('.content-body');
                                    let mapId = 'location-site-map-' + data['modules'].join();
                                    map.attr('id', mapId);
                                    map.css('height', '100%');
                                    self.renderMap(data, mapId);
                                    break;
                                }
                                case 'occurrence-charts': {
                                    self.createDataSummary(data, $container.find('.content-body'));
                                    break;
                                }
                                case 'survey': {
                                    self.createSurveyDataTable(data, $container);
                                    break;
                                }
                                case 'metadata-table': {
                                    self.renderMetadataTable(data, $container);
                                    break;
                                }
                                case 'taxa-chart': {
                                    self.createTaxaStackedBarChart($container);
                                    break;
                                }
                                case 'conservation-status-chart': {
                                    self.createConsStatusStackedBarChart(data, $container);
                                    break;
                                }
                                case 'endemism-chart': {
                                    self.createEndemismStackedBarChart($container);
                                    break;
                                }
                                case 'origin-chart': {
                                    self.createOriginStackedBarChart(data, $container);
                                    break;
                                }
                                case 'site-image': {
                                    $container.show();
                                    self.createSiteImageCarousel(data, $container);
                                    break;
                                }
                            }
                        })
                        self.loadingDashboard.hide();
                        return;
                    } else {
                        $dashboardWrapper.show();
                    }

                    if (data['is_multi_sites']) {
                        $('#species-ssdd-site-details').hide();
                        $('#ssdd-chem-chart-wrapper').hide();
                        self.createMultiSiteDetails(data);
                    } else {
                        $('#species-ssdd-site-details').show();
                        self.renderSingleSiteDetails(data);
                        if(data['is_chem_exists']) {
                            $('#ssdd-chem-chart-wrapper').show();
                            self.renderChemGraph(data);
                        }else {
                            $('#ssdd-chem-chart-wrapper').hide();
                        }
                    }
                    // Set default height for chart containers
                    $('#species-ssdd-occurrences-line-chart').css("height", 0.35 * screen.height + 'px')
                    $('#species-ssdd-taxa-occurrences-bar-chart').css("height", 0.35 * screen.height + 'px')
                    $('#species-ssdd-origin-bar-chart').css("height", 0.35 * screen.height + 'px')
                    $('#species-ssdd-endemism-bar-chart').css("height", 0.35 * screen.height + 'px')
                    $('#species-ssdd-cons-status-bar-chart').css("height", 0.35 * screen.height + 'px')
                    $('#species-ssdd-occurrence-data').css("height", 0.6 * screen.height + 'px')
                    // Render filter list
                    renderFilterList($('#filter-history-table'));
                    // Map
                    self.renderMap(data);
                    // Survey table
                    self.createSurveyDataTable(data);
                    // Summary charts
                    self.createDataSummary(data);
                    // Metadata table
                    self.renderMetadataTable(data);
                    // Occurrences bar chart
                    self.createOccurrencesBarChart(data);
                    // Occurrence data table
                    self.createOccurrenceDataTable(data);
                    // Taxa chart
                    self.createTaxaStackedBarChart();
                    // Conservation status chart
                    self.createConsStatusStackedBarChart(data);
                    // Endemism chart
                    self.createEndemismStackedBarChart();
                    // Origin chart
                    self.createOriginStackedBarChart(data);
                    if (!data['is_multi_sites']) {
                        self.createSiteImageCarousel(data);
                    }

                    self.loadingDashboard.hide();
                }
            });
        },
        fetchLocationSiteCoordinate: function (url) {
            var self = this;

            if (this.locationSiteCoordinateRequestXHR) {
                this.locationSiteCoordinateRequestXHR.abort();
                this.locationSiteCoordinateRequestXHR = null;
            }

            this.locationSiteCoordinateRequestXHR = $.get({
                url: url,
                dataType: 'json',
                success: function (data) {
                    var results = [];
                    if (data.hasOwnProperty('results')) {
                        results = data['results'];
                    }
                    $.each(results, function (index, siteData) {
                        self.drawMarkers(siteData);
                    });
                    if (self.uniqueSites.length === 1 && !data['next'] && !data['previous']) {
                        self.siteNameWrapper.show();
                        self.siteName.html(results[0].name);
                    }
                    self.locationSiteCoordinateRequestXHR = null;
                    self.fitSitesToMap();
                    if (data['next']) {
                        self.fetchLocationSiteCoordinate(data['next']);
                    }
                }
            });
        },
        fitSitesToMap: function () {
            var source = this.siteLayerVector.getSource();
            var extent = source.getExtent();
            this.mapLocationSite.getView().fit(extent, {
                size: this.mapLocationSite.getSize()
            });
        },
        drawMarkers: function (data) {
            var self = this;

            if (this.uniqueSites.includes(data['id'])) {
                return false;
            }
            this.uniqueSites.push(data['id']);

            // Create marker
            var coordinatesArray = data['coord'].split(',');
            var lon = parseFloat(coordinatesArray[0]);
            var lat = parseFloat(coordinatesArray[1]);
            coords = [lon, lat];
            var pos = ol.proj.fromLonLat(coords);

            var feature = new ol.Feature({
                geometry: new ol.geom.Point(
                    pos
                ),
                id: data['id'],
                name: data['name'],
            });

            this.siteLayerSource.addFeature(feature);
        },
        clearDashboard: function () {
            var self = this;
            this.$el.find('.chart-loading').show();
            let sassDashboardButton = $('#sass-dashboard-button');
            sassDashboardButton.hide();
            sassDashboardButton.removeClass('disabled');
            sassDashboardButton.attr('href', '#');
            $('#metadata-table-list').find('.content-body').html('');
            this.clearSiteImages();
            this.siteName.html('');
            this.siteNameWrapper.hide();
            this.uniqueSites = [];
            this.sourceReferenceIds = [];
            this.totalRecords.html('0');
            this.siteMarkers.html('');
            this.occurrenceData = {};
            this.occurrenceTable.html('<tr>\n' +
                '                            <th>Taxon</th>\n' +
                '                            <th>Category</th>\n' +
                '                            <th>Records</th>\n' +
                '                        </tr>');

            // Clear canvas
            if (this.originCategoryChart) {
                this.originCategoryChart.destroy();
                this.originCategoryChart = null;
            }

            if (this.recordsTimelineGraphCanvas) {
                this.recordsTimelineGraphCanvas.destroy();
                this.recordsTimelineGraphCanvas = null;
            }

            if (this.originTimelineGraphCanvas) {
                this.originTimelineGraphCanvas.destroy();
                this.originTimelineGraphCanvas = null;
            }

            if (this.taxaOccurrencesChartCanvas) {
                this.taxaOccurrencesChartCanvas.destroy();
                this.taxaOccurrencesChartCanvas = null;
            }

            if (this.consChartCanvas) {
                this.consChartCanvas.destroy();
                this.consChartCanvas = null;
            }

            if (this.originChartCanvas) {
                this.originChartCanvas.destroy();
                this.originChartCanvas = null;
            }

            if (this.endemismChartCanvas) {
                this.endemismChartCanvas.destroy();
                this.endemismChartCanvas = null;
            }

            // if (this.mapLocationSite) {
            //     this.mapLocationSite = null;
            //     $('#locationsite-map').html('');
            // }

            if (Shared.LocationSiteDetailXHRRequest) {
                Shared.LocationSiteDetailXHRRequest.abort();
                Shared.LocationSiteDetailXHRRequest = null;
            }

            if (this.locationSiteCoordinateRequestXHR) {
                this.locationSiteCoordinateRequestXHR.abort();
                this.locationSiteCoordinateRequestXHR = null;
            }

            if (this.grid) {
                $('.grid-stack').remove();
                this.grid.destroy();
                this.grid = null;
            }
        },
        closeDashboard: function () {
            if (!this.isOpen) {
                return false;
            }
            Shared.Dispatcher.trigger('bugReport:moveLeft');
            this.$el.find('#detailed-site-dashboard-wrapper')[0].scrollIntoView();
            var self = this;
            this.$el.hide('slide', {
                direction: 'right'
            }, 300, function () {
                self.isOpen = false;
                self.clearDashboard();
                if (self.currentDashbordWrapper) {
                    self.currentDashbordWrapper.html('');
                }
                self.loadingDashboard.show();
                let currentUrl = window.location.href;
                let newUrl = currentUrl.replace(/#site-detail\//, '#search//')
                    .replace(/siteId=[^&]*/, 'siteId=')
                    .replace(/siteIdOpen=[^&]*/, 'siteIdOpen=')
                    .replace(/modules=[^&]*/, 'modules=')
                Shared.Router.navigate(newUrl.split('#').at(-1), { trigger: false, replace: true });
            });
            this.$el.find('.dashboard-data-frequency').val('y');
        },
        exportLocationsiteMap: function () {
            $('.ol-control').hide();
            let title = 'Distribution Map'
            let mapElm = $('#locationsite-map');
            this.mapLocationSite.once('postrender', function (event) {
                showDownloadPopup('IMAGE', title, function (downloadRequestId) {
                    let canvas = mapElm[0];
                    let render = HtmlToCanvas(canvas, {
                        useCORS: false,
                        width: mapElm.width(),
                        background: '#FFFFFF',
                        allowTaint: true,
                        onrendered: function (canvas) {
                            canvas.toBlob(function (blob) {
                                uploadToDownloadRequest(downloadRequestId, blob, title + '.png');
                            }, 'image/png');
                        }
                    });
                    if (render && typeof render.then === 'function') {
                        render.then(function (canvas) {
                            canvas.toBlob(function (blob) {
                                uploadToDownloadRequest(downloadRequestId, blob, title + '.png');
                            }, 'image/png');
                        }).catch(function (err) {
                            console.error('HtmlToCanvas error:', err);
                        });
                    }

                    $('.ol-control').show();
                })
            });
            this.mapLocationSite.renderSync();
        },
        downloadOriginChart: function () {
            var title = 'site-origin-charts';
            var canvas = this.originCategoryGraph;
            this.downloadChart(title, canvas);
        },
        downloadRecordTimeline: function () {
            var title = 'record-timeline';
            var canvas = this.recordsTimelineGraph;
            this.downloadChart(title, canvas);
        },
        downloadCollectionTimeline: function () {
            var title = 'collection-timeline';
            var canvas = this.originTimelineGraph;
            this.downloadChart(title, canvas);
        },
        downloadChart: function (title, graph_canvas) {
            showDownloadPopup('CHART', title, function (downloadRequestId) {
                var img = new Image();
                var ctx = graph_canvas.getContext('2d');
                img.src = '/static/img/bims-stamp.png';
                img.onload = function () {
                    ctx.drawImage(img, graph_canvas.scrollWidth - img.width - 5,
                        graph_canvas.scrollHeight - img.height - 5);
                    var canvas = graph_canvas;
                    HtmlToCanvas(canvas, {
                        width: 10000,
                        height: 10000,
                        onrendered: function (canvas) {
                            canvas.toBlob(function (blob) {
                                uploadToDownloadRequest(downloadRequestId, blob, title + '.png');
                            }, 'image/png');
                        }
                    })
                }
            });
        },
        downloadElementEvent: function (button_el) {
            let that = this;
            let button = $(button_el.target);
            if (!button.hasClass('btn')) {
                button = button.parent();
            }
            let title = button.data('title');
            let titles = title.split(',');
            if (!title) {
                title = $(button).parent().find('.card-header-title').html().replaceAll(' ', '').replace(/(\r\n|\n|\r)/gm, '');
            }

            let chartNames = [];
            try {
                chartNames = button.data('chart').split(',');
            } catch (e) {
            }

            let target = button.data('datac');
            let element = that.$el.find('#' + target);
            let chartDownloaded = 0;

            if (chartNames.length > 0) {
                for (let i = 0; i < chartNames.length; i++) {
                    if (that.chartConfigs.hasOwnProperty(chartNames[i])) {
                        svgChartDownload(that.chartConfigs[chartNames[i]], titles[i]);
                        chartDownloaded += 1;
                    }
                }
                if (chartDownloaded === chartNames.length) {
                    return;
                }
                return;
            }
            if (element.length > 0)
                that.downloadElement(title, element);

        },
        downloadChartImage: function (e) {
            let button = $(e.target);
            if (!button.hasClass('btn')) {
                button = button.parent();
            }
            let target = button.data('datac');
            let title = button.data('title');
            let element = this.$el.find('.' + target);

            // Get image
            let image = element.children('img').attr('src');

            if (image) {
                showDownloadPopup('IMAGE', title, function (downloadRequestId) {
                    fetch(image)
                        .then(function (res) { return res.blob(); })
                        .then(function (blob) {
                            uploadToDownloadRequest(downloadRequestId, blob, title + '.png');
                        });
                });
            }
        },
        downloadElement: function (title, element) {
            var domElement = element[0];
            if (!domElement || !document.body.contains(domElement)) {
                return;
            }
            domElement.scrollIntoView();
            showDownloadPopup('TABLE', title, function (downloadRequestId) {
                // Re-check attachment after modal interaction
                if (!domElement || !document.body.contains(domElement)) {
                    alert('The element is no longer available. Please try again.');
                    return;
                }
                var render = HtmlToCanvas(domElement, {
                    height: 1000,
                    onrendered: function (canvas) {
                        canvas.toBlob(function (blob) {
                            uploadToDownloadRequest(downloadRequestId, blob, title + '.png');
                        }, 'image/png');
                    }
                });
                if (render && typeof render.then === 'function') {
                    render.then(function (canvas) {
                        canvas.toBlob(function (blob) {
                            uploadToDownloadRequest(downloadRequestId, blob, title + '.png');
                        }, 'image/png');
                    }).catch(function (err) {
                        console.error('HtmlToCanvas error:', err);
                    });
                }
            });
        },
        downloadingCSV: function (url, downloadButton, csv_name) {
            var self = this;
            self.downloadCSVXhr = $.get({
                url: url,
                dataType: 'json',
                success: function (data) {
                    if (data['status'] !== "success") {
                        if (data['status'] === "failed") {
                            if (self.downloadCSVXhr) {
                                self.downloadCSVXhr.abort();
                            }
                            let alertModalBody = $('#alertModalBody');

                            alertModalBody.html(data['message']);
                            $('#alertModal').modal({
                                'keyboard': false,
                                'backdrop': 'static'
                            });
                            downloadButton.html('Download as CSV');
                            downloadButton.prop("disabled", false);
                        } else {
                            setTimeout(
                                function () {
                                    self.downloadingCSV(url, downloadButton, csv_name);
                                }, 5000);
                        }
                    } else {
                        let a = window.document.createElement('a');
                        let filename;
                        if(data['filename']){
                            filename = data['filename']
                        }
                        else {
                            filename = data['message']
                        }
                        a.href = '/uploaded/processed_csv/' + filename;

                        a.click();
                        downloadButton.html('Download as CSV');
                        downloadButton.prop("disabled", false);
                    }
                },
                error: function (req, err) {
                }
            });
        },
        downloadAsCSV: function (e) {
            let button = $(e.target);
            let name = button.html();
            let self = this;
            button.html('Checking...');
            button.prop('disabled', true);
            let alertModalBody = $('#alertModalBody');
            if (!is_logged_in) {
                alertModalBody.html('Please log in first.')
                $('#alertModal').modal({
                    'keyboard': false,
                    'backdrop': 'static'
                });
            } else {
                showDownloadPopup('CSV', 'Occurrence Data', function (downloadRequestId) {
                    let url = self.csvDownloadEmailUrl;
                    if (!url.includes('?')) {
                        url += '?';
                    }
                    url += '&downloadRequestId=' + downloadRequestId
                    $.get({
                        url: url,
                        dataType: 'json',
                        success: function (data) {
                            if (data['status'] !== 'failed') {
                                alertModalBody.html(downloadRequestMessage);
                            } else {
                                alertModalBody.html(data['message'])
                            }
                            $('#alertModal').modal({
                                'keyboard': false,
                                'backdrop': 'static'
                            });
                        }
                    });
                }, false)
            }
            button.html(name);
            button.prop('disabled', false);
        },
        publishLiveLayer: function () {
            OgcLiveLayer.open({
                query: this.currentFiltersUrl,
                name: 'Live layer ' + new Date().toISOString().slice(0, 10)
            });
        },
        downloadChecklist: function (e) {
            let csvName = 'Checklist';
            let button = $(e.target);
            let self = this;
            let name = button.html();

            let alertModalBody = $('#alertModalBody');
            button.html('Processing...');
            button.prop('disabled', true);

            if (!is_logged_in) {
                alertModalBody.html('Please log in first.')
                $('#alertModal').modal({
                    'keyboard': false,
                    'backdrop': 'static'
                }).on('hidden.bs.modal', function () {
                    button.html(name);
                    button.prop('disabled', false);
                });
            } else {
                showDownloadPopup('PDF', csvName, function (downloadRequestId) {
                    let url = self.checklistDownloadUrl;
                    $.ajax({
                        url: url,
                        dataType: 'json',
                        type: 'POST',
                        headers: {
                            'X-CSRFToken': csrfmiddlewaretoken
                        },
                        data: {
                            'downloadRequestId': downloadRequestId
                        },
                        success: function (data) {
                            $('#alertModal').modal({
                                'keyboard': false,
                                'backdrop': 'static'
                            });
                            if (data['status'] === 'failed') {
                                let errorMessage = 'Unexpected Error'
                                if (data['message']) {
                                    errorMessage = data['message']
                                }
                                alertModalBody.html('ERROR : ' + errorMessage);
                            } else {
                                alertModalBody.html(downloadRequestMessage);
                            }
                            button.html(name);
                            button.prop('disabled', false);
                        }
                    });
                }, false, function () {
                    button.html('Download checklist');
                    button.prop('disabled', false);
                })
            }
        },
        downloadGBIfIds: function (e){
            let csv_name = 'GBIF ids'
            let button = $(e.target);
            let self = this;
            let name = button.html();

            let alertModalBody = $('#alertModalBody');
            button.html('Checking...');
            button.prop('disabled', true);

            if (!is_logged_in) {
                alertModalBody.html('Please log in first.')
                $('#alertModal').modal({
                    'keyboard': false,
                    'backdrop': 'static'
                });
            } else {
                showDownloadPopup('CSV', csv_name, function (downloadRequestId) {
                    let url = self.csvDownloadGbifIdsUrl;
                    if (!url.includes('?')) {
                        url += '?';
                    }
                    url += '&downloadRequestId=' + downloadRequestId
                    $.get({
                        url: url,
                        dataType: 'json',
                        success: function (data) {
                            $('#alertModal').modal({
                                'keyboard': false,
                                'backdrop': 'static'
                            });
                            if (data['status'] === 'failed') {
                                let errorMessage = 'Unexpected Error'
                                if (data['message']) {
                                    errorMessage = data['message']
                                }
                                alertModalBody.html('ERROR : ' + errorMessage);
                            } else {
                                alertModalBody.html(downloadRequestMessage);
                            }
                        }
                    });
                }, false);
            }
            button.html(name);
            button.prop('disabled', false);
        },
        downloadChemRecordsAsCSV: function (e) {
            let button = $(e.target);
            let that = this;
            showDownloadPopup('CSV', 'Chemical Records', function (downloadRequestId) {
                button.html('Processing...');
                button.prop("disabled", true);
                that.downloadingCSV(that.chemCsvDownloadUrl, button, 'ChemicalRecords');
            });
        },
        renderStackedBarChart: function (dataIn, chartName, chartCanvas, randomColor = false) {
            if (!(dataIn.hasOwnProperty('data'))) {
                return false;
            }
            var datasets = [];
            var barChartData = {};
            let colorMap = {}
            if (dataIn.hasOwnProperty('colours')) {
                colorMap = dataIn['colours'];
            }
            let colours = this.chartColors || [
                '#D7CD47', '#8D2641', '#18A090', '#3D647D', '#B77282', '#E6E188', '#6BC0B5', '#859FAC'
            ];
            var myDataset = {};
            var count = dataIn['dataset_labels'].length;
            for (let i = 0; i < count; i++) {
                myDataset = {};
                var nextKey = dataIn['dataset_labels'][i];
                var nextColour = colours[i%colours.length];

                if (colorMap && colorMap.hasOwnProperty(nextKey)) {
                    nextColour = colorMap[nextKey];
                } else {
                    if (randomColor) {
                        nextColour = Shared.ColorUtil.generateHexColor();
                    }
                }
                var nextData = dataIn['data'][nextKey];
                myDataset = {
                    'label': nextKey,
                    'backgroundColor': nextColour,
                    'data': nextData
                };
                datasets.push(myDataset);
            }
            barChartData = {
                'labels': dataIn['labels'],
                'datasets': datasets,
            };
            let chartConfig = {
                type: 'bar',
                data: barChartData,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    legend: {display: true},
                    title: {display: false},
                    hover: {mode: 'point', intersect: false},
                    tooltips: {
                        mode: 'point',
                        position: 'average',
                    },
                    borderWidth: 0,
                    scales: {
                        xAxes: [{
                            stacked: true,
                        }],
                        yAxes: [{
                            stacked: true,
                            ticks: {
                                beginAtZero: true,
                                callback: function (value) {
                                    if (value % 1 === 0) {
                                        return value;
                                    }
                                },
                            },
                            scaleLabel: {
                                display: true,
                                labelString: 'Occurrences',
                            },
                        }]
                    }
                }

            };
            chartCanvas = this.resetCanvas(chartCanvas);
            var ctx = chartCanvas.getContext('2d');
            this.chartConfigs[chartName] = chartConfig;
            return new ChartJs(ctx, chartConfig);
        },
        fetchChartData: function (chartContainer, baseUrl, callback) {
            let width = chartContainer.width();
            width += 150; // padding
            let loadingChart = chartContainer.find('.chart-loading');
            loadingChart.show();
            baseUrl += this.currentFiltersUrl;
            baseUrl += '&width=' + width;
            baseUrl += '&base_64=1';
            $.get({
                url: baseUrl,
                cache: true,
                processData: false,
                success: function (data) {
                    loadingChart.hide();
                    callback(data);
                },
                error: function () {
                    loadingChart.hide();
                    chartContainer.html('No Data');
                }
            });
        },
        createOriginStackedBarChart: function (data, container = null) {
            let self = this;
            let originCategoryList = data['origin_name_list'];
            if (!container) {
                container = this.$el.find('#species-ssdd-origin-bar-chart');
            }
            let baseUrl = '/api/location-sites-occurrences-chart-data/';
            let canvasContainer = container.find('.canvas-container');
            let chartCanvas = container.find('canvas')[0];
            canvasContainer.find('canvas').hide();

            this.fetchChartData(
                container,
                baseUrl,
                (responseData) => {
                    if (Object.keys(responseData['data']).length === 0) {
                        canvasContainer.hide();
                        return;
                    }
                    canvasContainer.show();
                    // Update labels
                    $.each(responseData['dataset_labels'], function (index, label) {
                        if (originCategoryList.hasOwnProperty(label)) {
                            responseData['dataset_labels'][index] = originCategoryList[label];
                        }
                    });
                    $.each(responseData['data'], function (key, current_data) {
                        if (originCategoryList.hasOwnProperty(key)) {
                            delete responseData['data'][key];
                            responseData['data'][originCategoryList[key]] = current_data;
                        }
                    });
                    this.originChartCanvas = self.renderStackedBarChart(responseData, 'origin_bar', chartCanvas);
                }
            );
        },
        createSiteImageCarousel: function (data, container) {
            let siteImages = data['site_images'];
            if (!container) {
                container = this.$el.find('#ssdd-carousel-wrapper');
                if (siteImages.length > 0) {
                    container.show();
                }
            } else {
                if (siteImages.length === 0) {
                    container.find('.content-body').html(`<div class="center-text">No site image</div>`);
                }
            }
            $.each(siteImages, function (index, siteImage) {
                let className = '';
                if (index === 0) {
                    className = 'active'
                }
                container.find('.carousel-indicators').append(
                    '<li data-target="#ssdd-site-image-carousel" data-slide-to="'+ index +'" class="'+ className +'"/>'
                );
                container.find('.carousel-inner').append(
                    '<div class="carousel-item '+ className +'" style="height: 100%">' +
                    '  <img alt="" src="' + siteImage + '" height="100%">' +
                    '</div>'
                );
            });
        },
        clearSiteImages: function () {
            let wrapper = this.$el.find('#ssdd-carousel-wrapper');
            wrapper.hide();
            wrapper.find('.carousel-indicators').html('');
            wrapper.find('.carousel-inner').html('');
        },
        createTaxaStackedBarChart: function (container = null) {
            let self = this;
            if (!container) {
                container = this.$el.find('#species-ssdd-taxa-occurrences-bar-chart');
            }
            let baseUrl = '/api/location-sites-taxa-chart-data/';
            let chartCanvas = container.find('canvas')[0];
            let canvasContainer = container.find('.canvas-container');
            canvasContainer.find('canvas').hide();

            this.fetchChartData(
                container,
                baseUrl,
                (responseData) => {
                    if(Object.keys(responseData['data']).length === 0) {
                        canvasContainer.hide();
                        return;
                    }
                    canvasContainer.show();
                    this.taxaOccurrencesChartCanvas = self.renderStackedBarChart(responseData, 'taxa_stacked_bar', chartCanvas);
                }
            )
        },
        createConsStatusStackedBarChart: function (data, container = null) {
            let self = this;
            let iucnCategoryList = data['iucn_name_list'];
            if (!container) {
                container = this.$el.find('#species-ssdd-cons-status-bar-chart');
                container.find('')
            }
            let baseUrl = '/api/location-sites-cons-chart-data/';
            let canvasContainer = container.find('.canvas-container');
            canvasContainer.find('canvas').hide();

            this.fetchChartData(
                container,
                baseUrl,
                (responseData) => {
                    if (Object.keys(responseData['data']).length === 0) {
                        canvasContainer.hide();
                        return;
                    }
                    canvasContainer.show();
                    // Update labels
                    $.each(responseData['dataset_labels'], function (index, label) {
                        if (iucnCategoryList.hasOwnProperty(label)) {
                            responseData['colours'][iucnCategoryList[label]] = responseData['colours'][responseData['dataset_labels'][index]]
                            responseData['dataset_labels'][index] = iucnCategoryList[label];
                        }
                    });
                    // Update data title
                    $.each(responseData['data'], function (key, data) {
                        if (iucnCategoryList.hasOwnProperty(key)) {
                            delete responseData['data'][key];
                            responseData['data'][iucnCategoryList[key]] = data;
                        }
                    });
                    let chartCanvas = container.find('canvas')[0];
                    this.consChartCanvas = self.renderStackedBarChart(
                        responseData, 'cons_status_bar', chartCanvas);
                }
            )
        },
        createEndemismStackedBarChart: function (container) {
            let self = this;
            if (!container) {
                container = this.$el.find('#species-ssdd-endemism-bar-chart');
            }
            let baseUrl = '/api/location-sites-endemism-chart-data/';
            let chartCanvas = container.find('canvas')[0];
            let chartContainer = container.find('.canvas-container');
            chartContainer.find('canvas').hide();

            this.fetchChartData(
                container,
                baseUrl,
                (responseData) => {
                    if(Object.keys(responseData['data']).length === 0) {
                        chartContainer.hide();
                        return;
                    }
                    chartContainer.show();
                    this.endemismChartCanvas = self.renderStackedBarChart(responseData, 'endemism', chartCanvas);
                }
            )
        },
        renderBarChart: function (data_in, chartName, chartCanvas) {

            if (!(data_in.hasOwnProperty(chartName + '_chart'))) {
                return false;
            };

            var chartConfig = {
                type: 'bar',
                data: {
                    datasets: [{
                        data: data_in[chartName + '_chart']['values'],
                        backgroundColor: this.chartColors[0] || '#D7CD47',
                        borderColor: this.chartColors[0] || '#D7CD47',
                        fill: false
                    }],
                    labels: data_in[chartName + '_chart']['keys']
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    legend: {display: false},
                    title: {display: false},
                    hover: {mode: 'point', intersect: false},
                    tooltips: {
                        mode: 'point',
                    },
                    borderWidth: 0,
                    scales: {
                        xAxes: [{
                            display: true,
                            scaleLabel: {
                                display: false,
                                labelString: ''
                            }
                        }],

                        yAxes: [{
                            display: true,
                            scaleLabel: {
                                display: true,
                                labelString: data_in[chartName + '_chart']['title']
                            },
                            ticks: {
                                beginAtZero: true,
                                callback: function (value) {
                                    if (value % 1 === 0) {
                                        return value;
                                    }
                                }
                            }
                        }]
                    }
                }
            };
            chartCanvas = this.resetCanvas(chartCanvas);
            this.chartConfigs[chartName] = chartConfig;
            var ctx = chartCanvas.getContext('2d');
            ctx.height = 100;
            new ChartJs(ctx, chartConfig);
        },
        createOccurrencesBarChart: function (data, chartCanvas) {
            if (!chartCanvas) {
                chartCanvas = document.getElementById('species-ssdd-occurrences-line-chart-canvas');
            }
            let container = $(chartCanvas);
            let _maxTry = 10;
            let _currentTry = 1;
            while (!container.hasClass('card-body') && _currentTry < _maxTry) {
                container = container.parent();
                _currentTry =+ 1;
            }
            let cardHeight = container.height();
            $(chartCanvas).parent().css('height', cardHeight);

            if (data.hasOwnProperty('taxa_occurrence')) {
                if (data['taxa_occurrence']['occurrences_line_chart']['values'].length === 0) {
                    this.$el.find('.species-ssdd-occurrences-line-chart').hide();
                    return;
                }
                this.$el.find('.species-ssdd-occurrences-line-chart').show();
                this.renderBarChart(data['taxa_occurrence'], 'occurrences_line', chartCanvas);
            }
        },
        createMultiSiteDetails: function (data, element) {
            if (!element) {
                element = this.$el.find('#records-sites');
            }
            element.html(this.renderTableFromTitlesValuesLists(data['site_details']['overview']));
            this.createOriginsOccurrenceTable(data);
            this.createConservationOccurrenceTable(data);
            this.createEndemismOccurrenceTable(data);
        },
        renderSingleSiteDetails: function (data, element = null) {
            if (!element) {

            }
            let siteDetailsWrapper = $('#species-ssdd-site-details');
            const siteDetailsData = data['site_details']
            siteDetailsWrapper.html('');
            const container =  _.template($('#site-details-container').html());
            $.each(siteDetailsData, (key, data) => {
                const containerHtml = container({
                    title: key,
                    detailsData: data
                })
                siteDetailsWrapper.append(containerHtml);
            })
            this.createOriginsOccurrenceTable(data);
            this.createConservationOccurrenceTable(data);
            this.createEndemismOccurrenceTable(data);
        },
        createOriginsOccurrenceTable: function (data) {
            let originsSub = this.$el.find('#origins');
            let originCategoryList = data['origin_name_list'];
            let originChartData = data['biodiversity_data']['species']['origin_chart'];
            let originsTableData = {};
            $.each(originChartData['keys'], function (index, value) {
                let category = value;
                if(originCategoryList.hasOwnProperty(value)) {
                    category = originCategoryList[value];
                }
                originsTableData[category] = originChartData['data'][index];
            });
            originsSub.html(this.renderTableFromTitlesValuesLists(originsTableData, false));
        },
        createConservationOccurrenceTable: function (data) {
            let self = this;
            let conservation_statusSub = this.$el.find('#ssdd-conservation-status');
            let consChartData = data['biodiversity_data']['species']['cons_status_chart'];
            let consCategoryList = data['iucn_name_list'];
            let constTableData = {};
            $.each(consChartData['keys'], function (index, value) {
                let category = value;
                if(consCategoryList.hasOwnProperty(value)) {
                    category = consCategoryList[value];
                }
                category = self.titleCase(category);
                constTableData[category] = consChartData['data'][index];
            });
            constTableData = this.sortOnKeys(constTableData);
            conservation_statusSub.html(this.renderTableFromTitlesValuesLists(constTableData, false));
        },
        createEndemismOccurrenceTable: function (data) {
            let self = this;
            let endemismDataChart = data['biodiversity_data']['species']['endemism_chart'];
            let endemismData = {};
            if (!endemismDataChart) {
                return false;
            }
            $.each(endemismDataChart['keys'], function (index, data) {
                var titleData = self.titleCase(data);
                endemismData[titleData] = endemismDataChart['data'][index];
            });
            let wrapper = this.$el.find('#ssdd-endemism');
            wrapper.html(this.renderTableFromTitlesValuesLists(endemismData, false));
        },
        renderSiteDetailInfo: function (data) {
            var $detailWrapper = $('<div></div>');
            if (data.hasOwnProperty('site_detail_info')) {
                let siteDetailsTemplate = _.template($('#site-details-template').html());
                $detailWrapper.html(siteDetailsTemplate(data));
            }
            return $detailWrapper;
        },
        getIUCN_name: function(data, chartName){
            let iucnCategory = data['iucn_name_list'];
            let biodiversityData = data['biodiversity_data']['species'];
            for (let i = 0; i < biodiversityData[chartName]['keys'].length; i++) {
                let next_name = biodiversityData[chartName]['keys'][i];
                if (iucnCategory.hasOwnProperty(next_name)) {
                    biodiversityData[chartName]['keys'][i] = iucnCategory[next_name];
                }
            }
            return biodiversityData[chartName]['keys'];
        },

        createDataSummary: function (data, container = null, height = null) {
            let bio_data = data['biodiversity_data'];
            let nationalConsExist = bio_data['species'].hasOwnProperty('cons_status_national_chart');
            let biodiversityData = data['biodiversity_data']['species'];
            let origin_length = biodiversityData['origin_chart']['keys'].length;
            let originNameList = data['origin_name_list'];
            for (let i = 0; i < origin_length; i++) {
                let next_name = biodiversityData['origin_chart']['keys'][i];
                if (originNameList.hasOwnProperty(next_name)) {
                    biodiversityData['origin_chart']['keys'][i] = originNameList[next_name];
                }
            }

            this.getIUCN_name(
                data,
                'cons_status_chart',
            )

            if (nationalConsExist) {
                this.getIUCN_name(
                    data,
                    'cons_status_national_chart',
                )
            }

            let originPieCanvas = document.getElementById('species-ssdd-origin-pie');
            let endemismPieCanvas = document.getElementById('species-ssdd-endemism-pie');
            let conservationStatusPieCanvas = document.getElementById('species-ssdd-conservation-status-pie');
            let samplingMethodPieCanvas = document.getElementById('species-ssdd-sampling-method-pie');
            let biotopeCanvas = document.getElementById('species-ssdd-biotope-pie');
            let conservationStatusNationalPieCanvas = document.getElementById('species-ssdd-conservation-status-national-pie');

            if (container) {
                let parentHeight = container.parent().height();
                let titleHeight = container.find('.ssdd-titles').height();
                let legendHeight = container.find('.species-ssdd-legend').height();
                let padding = 90;
                container.css('height', parentHeight);
                container.find('.col-chart').css('height', parentHeight - titleHeight - legendHeight - padding);

                originPieCanvas = container.find('.occurrence-origin-chart').find('canvas')[0];
                endemismPieCanvas = container.find('.occurrence-endemism-chart').find('canvas')[0];
                conservationStatusPieCanvas = container.find('.occurrence-conservation-status-chart').find('canvas')[0];
                samplingMethodPieCanvas = container.find('.occurrence-sampling-method-chart').find('canvas')[0];
                biotopeCanvas = container.find('.occurrence-biotope-chart').find('canvas')[0];
                conservationStatusNationalPieCanvas = container.find('.occurrence-conservation-status-national-chart').find('canvas')[0];
            }

            this.renderPieChart(bio_data, 'species', 'origin', originPieCanvas);
            this.renderPieChart(bio_data, 'species', 'endemism', endemismPieCanvas);
            this.renderPieChart(bio_data, 'species', 'cons_status', conservationStatusPieCanvas);
            this.renderPieChart(bio_data, 'species', 'sampling_method', samplingMethodPieCanvas);
            this.renderPieChart(bio_data, 'species', 'biotope', biotopeCanvas);
            if (nationalConsExist) {
                this.renderPieChart(bio_data, 'species', 'cons_status_national', conservationStatusNationalPieCanvas);
            } else {
                document.querySelector('.occurrence-conservation-status-national-chart').style.display = 'none';
                document.querySelector('.cons-status-national-title').style.display = 'none';
                document.querySelector('#species-ssdd-cons_status_national-legend').style.display = 'none';
            }
        },
        renderPieChart: function (data, speciesType, chartName, chartCanvas) {
            if (typeof data == 'undefined') {
                return null;
            }

            let chartData = data[speciesType][chartName + '_chart']

            // Use chart colors from API or fallback to default
            let chartColours = this.chartColors || [
                '#8D2641', '#641f30',
                '#E6E188', '#D7CD47',
                '#9D9739', '#525351',
                '#618295', '#2C495A',
                '#39B2A3', '#17766B',
                '#859FAC', '#1E2F38'
            ];

            if (chartData.hasOwnProperty('colours')) {
                chartColours = chartData['colours'];
            }

            var chartConfig = {
                type: 'pie',
                data: {
                    datasets: [{
                        data: chartData['data'],
                        backgroundColor: chartColours
                    }],
                    labels: chartData['keys']
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    legend: {display: false},
                    title: {display: false},
                    hover: {mode: 'nearest', intersect: false},
                    borderWidth: 0,
                }
            };
            chartCanvas = this.resetCanvas(chartCanvas);
            var ctx = chartCanvas.getContext('2d');
            new ChartJs(ctx, chartConfig);
            this.chartConfigs[chartName + '_pie'] = chartConfig;

            // Render chart labels
            var dataKeys = chartData['keys'];
            var dataLength = dataKeys.length;
            var chart_labels = {};
            chart_labels[chartName] = '';
            for (var i = 0; i < dataLength; i++) {
                chart_labels[chartName] += '<div><span style="color:' +
                    chartColours[i] + ';">■</span>' +
                    '<span class="species-ssdd-legend-title">&nbsp;' +
                    dataKeys[i] + '</span></div>'
            }
            var element_name = `#species-ssdd-${chartName}-legend`;
            $(element_name).html(chart_labels[chartName]);
        },
        renderTableFromTitlesValuesLists: function (specific_data, bold_title = false) {
            var temp_result;
            var title_class = '';
            var $result = $('<div></div>');
            if (bold_title === true) {
                title_class = 'title_column';
            }
            $.each(specific_data, function (key, value) {
                temp_result = `<div class="row">
                               <div class="col-6 ${title_class}">${key}</div>
                               <div class="col-6">${value}</div>
                               </div>`;
                $result.append(temp_result);
            });
            return $result;
        },
        createSurveyDataTable: function (data, container) {
            if (!container) {
                container = $('#species-ssdd-survey-data');
            }
            let $dataWrapper = container.find('.content-body');
            let $surveyButton = container.find('.survey-button');
            let $surveyInfo = container.find('.survey-info');
            $dataWrapper.html('');
            if (!data.hasOwnProperty('survey')) return false;
            if (data['survey'].length > 0) {
                $.each(data['survey'], function (index, survey_data){
                    $dataWrapper.append(
                        `<tr>
                            <td><a href="/site-visit/detail/${survey_data['id']}/">${survey_data['site']}</a></td>
                            <td>${survey_data['date']}</td>
                            <td>${survey_data['records']}</td>
                        </tr>`
                    )
                });
            }
            if (data['total_survey'] > 5) {
                $surveyButton.show();
                $surveyInfo.show();
            } else {
                $surveyButton.hide();
                $surveyInfo.hide();
            }
            $surveyButton.click(function() {
                let url = window.location.href;
                url = url.split('site-detail/');
                if (url <= 1) {
                    return false;
                }
                window.location.href = `/site-visit/list/?${url[1]}`;
                return true;
            })
        },
        createOccurrenceDataTable: function (data, container = null) {
            if (!container) {
                container = $('#species-ssdd-occurrence-data');
            }
            // let occurrenceDataSub = occurrenceDataWrapper.find('#occurrence-data');
            let occurrenceDataSub = container.find('.content-body');
            if (data['occurrence_data'].length > 0 || data['iucn_name_list'].length > 0 || data['origin_name_list'].length > 0) {
                this.$el.find('.download-as-csv').show();
                occurrenceDataSub.html(this.renderOccurrenceData(data['occurrence_data'], data['iucn_name_list'], data['origin_name_list']));
            } else {
                occurrenceDataSub.html('No Data');
                this.$el.find('.download-as-csv').hide();
            }
        },
        downloadCitationList: function (e) {
            e.preventDefault();
            let format = $(e.currentTarget).data('format');
            let self = this;
            let alertModalBody = $('#alertModalBody');

            if (!is_logged_in) {
                alertModalBody.html('Please log in first.');
                $('#alertModal').modal({'keyboard': false, 'backdrop': 'static'});
                return;
            }

            let hasRefs = self.sourceReferenceIds && self.sourceReferenceIds.length > 0;
            let hasDatasets = self.datasetIds && self.datasetIds.length > 0;
            if (!hasRefs && !hasDatasets) {
                alertModalBody.html('No source references available to download.');
                $('#alertModal').modal({'keyboard': false, 'backdrop': 'static'});
                return;
            }

            showDownloadPopup('CSV', 'Citation List', function (downloadRequestId) {
                let formData = new FormData();
                formData.append('citation_format', format);
                formData.append('download_request_id', downloadRequestId);
                $.each(self.sourceReferenceIds || [], function (i, id) {
                    formData.append('source_reference_ids', id);
                });
                $.each(self.datasetIds || [], function (i, id) {
                    formData.append('dataset_ids', id);
                });

                $.ajax({
                    url: '/api/download-citations/',
                    type: 'POST',
                    headers: {'X-CSRFToken': csrfmiddlewaretoken},
                    data: formData,
                    processData: false,
                    contentType: false,
                    success: function () {
                        alertModalBody.html(
                            'Your citation list is being generated and will be emailed to you shortly.'
                        );
                        $('#alertModal').modal({'keyboard': false, 'backdrop': 'static'});
                    },
                    error: function (xhr) {
                        let msg = 'Failed to generate citation list. Please try again.';
                        if (xhr.responseJSON && xhr.responseJSON.error) {
                            msg = xhr.responseJSON.error;
                        }
                        alertModalBody.html(msg);
                        $('#alertModal').modal({'keyboard': false, 'backdrop': 'static'});
                    }
                });
            }, true, null, false);
        },
        renderMetadataTable: function (data, container=null) {
            if (!container) {
                container = $('#ssdd-metadata-table');
            }
            let ulDiv = container.find('.content-body');
            ulDiv.html(' ');
            let dataSources = data['source_references'];
            this.sourceReferenceIds = dataSources
                .filter(function (s) { return s['Reference Category'] !== 'Occurrence dataset'; })
                .map(function (s) { return s['ID']; });
            this.datasetIds = dataSources
                .filter(function (s) { return s['Reference Category'] === 'Occurrence dataset'; })
                .map(function (s) { return s['ID']; });
            let order = ['is_doc', 'Reference Category', 'Author/s', 'Year', 'Title', 'Source', 'DOI/URL', 'Notes'];
            let hiddenKeys = ['is_doc']; // Don't show this key in table
            let orderedDataSources = [];
            for (let j=0; j<dataSources.length; j++) {
                orderedDataSources.push({});
                for (let i = 0; i < order.length; i++) {
                    orderedDataSources[j][order[i]] = dataSources[j][order[i]];
                }
            }

            let headerDiv = $('<thead><tr></tr></thead>');
            if(orderedDataSources.length > 0) {
                let keys = Object.keys(orderedDataSources[0]);
                for (let i = 0; i < keys.length; i++) {
                    if (hiddenKeys.includes(keys[i])) {
                        continue;
                    }
                    headerDiv.append('<th>' + keys[i] + '</th>')
                }
            }
            ulDiv.append(headerDiv);

            let bodyDiv = $('<tbody></tbody>');
            $.each(orderedDataSources, function (index, source) {
                let itemDiv = $('<tr></tr>');
                let keys = Object.keys(source);
                let document = false;
                for(let i=0; i<keys.length; i++){
                    if (keys[i] === 'is_doc') {
                        if (source[keys[i]]) {
                            document = true;
                        }
                        continue;
                    }
                    if (keys[i] === 'Author/s') {
                        if (Array.isArray(source[keys[i]]) && source[keys[i]].length > 1) {
                            itemDiv.append(`<td>${source[keys[i]].join(', ')}</td>`);
                            continue;
                        }
                    }
                    if(keys[i] === 'DOI/URL' && document && source[keys[i]] && source[keys[i]].indexOf('/uploaded/') > -1) {
                        itemDiv.append('<td><a href="'+ source[keys[i]] + '" target="_blank">Download</a></td>')
                    } else if (keys[i] === 'DOI/URL' && source[keys[i]] && source[keys[i]].substring(0, 4) !== 'http') {
                        itemDiv.append(`<td><a href="http://dx.doi.org/${source[keys[i]]}" target="_blank">${source[keys[i]]}</a></td>`);
                    } else {
                        if (keys[i] === 'DOI/URL' && source[keys[i]] && source[keys[i]].substring(0, 4) === 'http') {
                            itemDiv.append(`<td><a href="${source[keys[i]]}" target="_blank">${source[keys[i]]}<a/></td>`);
                        } else {
                            itemDiv.append('<td>' + (source[keys[i]] ? source[keys[i]] : '-') + '</td>')
                        }
                    }
                }
                bodyDiv.append(itemDiv);
            });
            ulDiv.append(bodyDiv);
        },
        renderOccurrenceData: function (occurrenceData, conservationStatusList, originCategoryList) {
            let occurrenceTable = $('<table class="table table-bordered table-condensed table-sm site-detailed-table">');
            occurrenceTable.append("<thead>\n" +
                "      <tr>\n" +
                "        <th>Taxon</th>\n" +
                "        <th>Occurrences</th>\n" +
                "        <th>Origin</th>\n" +
                "        <th>Endemism</th>\n" +
                "        <th>Cons. Status (Global)</th>\n" +
                "      </tr>\n" +
                "    </thead>");
            let tableBody = $('<tbody>');
            $.each(occurrenceData, function (index, rowData) {
                let tRow = $('<tr>');
                let originName = rowData['origin'];
                if (originCategoryList.hasOwnProperty(originName)) {
                    originName = originCategoryList[originName];
                }
                let consName = rowData['cons_status'];
                if (conservationStatusList.hasOwnProperty(consName)) {
                    consName = conservationStatusList[consName];
                } else {
                    consName = 'Not evaluated';
                }
                tRow.append('<td>' + rowData['taxon'] + '</td>');
                tRow.append('<td>' + rowData['count'] + '</td>');
                tRow.append('<td>' + originName + '</td>');
                tRow.append('<td>' + rowData['endemism'] + '</td>');
                tRow.append('<td>' + consName + '</td>');
                tableBody.append(tRow);
            });
            occurrenceTable.append(tableBody);
            return occurrenceTable;
        },
        resetCanvas: function (chartCanvas) {
            var chartParent = chartCanvas.parentElement;
            var newCanvas = document.createElement("CANVAS");
            var chartId = chartCanvas.id;
            newCanvas.id = chartId;
            chartCanvas.remove();
            chartParent.append(newCanvas);
            return document.getElementById(chartId);
        },
        sortOnKeys: function (dict) {
            var sorted = [];
            for (var key in dict) {
                sorted[sorted.length] = key;
            }
            sorted.sort();
            var tempDict = {};
            for (var i = 0; i < sorted.length; i++) {
                tempDict[sorted[i]] = dict[sorted[i]];
            }
            return tempDict;
        },
        titleCase: function (str) {
            var splitStr = str.toLowerCase().split(' ');
            for (var i = 0; i < splitStr.length; i++) {
               splitStr[i] = splitStr[i].charAt(0).toUpperCase() + splitStr[i].substring(1);
            }
            return splitStr.join(' ');
        },
        renderChemGraph: function (data) {
            var self = this;
            var $chemWrapper = $('#species-ssdd-chem-bar-chart');
            $chemWrapper.html('');
            var xLabel = data['chemical_records']['x_label'];
            delete data['chemical_records']['x_label'];

            xLabel.sort(function(a, b) {
                a = new Date(a);
                b = new Date(b);
                return a < b ? -1 : a > b ? 1 : 0;
            });

            $.each(data['chemical_records'], function (key, value) {
                var id_canvas = key + '-chem-chart';
                var canvas = '<canvas class="chem-bar-chart" id="' + id_canvas + '"></canvas>';
                $chemWrapper.append(canvas);
                var ctx = document.getElementById(id_canvas).getContext('2d');
                var datasets = [];
                var yLabel;
                var xLabelData = [];
                $.each(value, function (idx, val) {
                    var key_item = Object.keys(val)[0];
                    if(key_item.toLowerCase().indexOf('max') === -1 && key_item.toLowerCase().indexOf('min') === -1){
                        let unit = '';
                        if (val[key_item]['name'].toLowerCase() !== 'ph') {
                            unit = ' (' + val[key_item]['unit'] + ')';
                        }
                        yLabel = val[key_item]['name'] + unit;
                    }
                    var value_data = val[key_item]['values'];
                    var graph_data = [];
                    for(var i=0; i<value_data.length; i++){
                        graph_data.push({
                            y: value_data[i]['value'],
                            x: value_data[i]['str_date']
                        });
                        xLabelData.push(value_data[i]['str_date'])
                    }
                    datasets.push({
                        label: key_item,
                        data: graph_data,
                        backgroundColor: '#cfdeea',
                        borderColor: '#cfdeea',
                        borderWidth: 2,
                        fill: false
                    })
                });

                xLabelData.sort(function(a, b) {
                    a = new Date(a);
                    b = new Date(b);
                    return a < b ? -1 : a > b ? 1 : 0;
                });
                var _data = {
                    labels: xLabelData,
                    datasets: datasets
                };
                var options= {
					responsive: true,
					hoverMode: 'index',
					stacked: false,
					title: {
						display: false
					},
                    legend: {
                        display: false,
                    },
					scales: {
						yAxes: [{
						    scaleLabel: {
                                display: true,
                                labelString: yLabel
                            },
							type: 'linear',
							display: true,
							position: 'left',
                            ticks: {
                                beginAtZero: true
                            }
						}]
					}
				};
                var chartConfig = {
                    type: 'bar',
                    data: _data,
                    options: options
                };
                self.chartConfigs[id_canvas] = chartConfig;
                new ChartJs(ctx, chartConfig)
            })
        },
        downloadChemGraphs: function () {
            var self = this;
            let button = $('#chem-graph-export');
            let titles = button.data('title');
            var elements = $('#species-ssdd-chem-bar-chart .chem-bar-chart');
            for(var i=0; i<elements.length; i++){
                var title = titles + ' - ' + $(elements[i]).attr('id').split('-')[0];
                var canvas = $(elements[i]).attr('id')
                svgChartDownload(self.chartConfigs[canvas], title);
            }
        },
        renderCustomDashboardLayout: function ($wrapper, dashboardConfiguration) {
            $.each(dashboardConfiguration, (index, configuration) => {
                // Find the container
                let $container = '';
                $container = this.$el.find(`.${configuration['key']}`).clone();
                if (configuration['width'] === 12) {
                    $container.addClass('mr-4');
                    $container.addClass('ml-2');
                } else {
                    $container.addClass('mr-2');
                    $container.addClass('ml-2');
                }
                $container.addClass('card-100');
                $container.addClass('grid-stack-item-content');
                let $div = $(`<div class="grid-stack-item" data-gs-x="${configuration['x']}" data-gs-y="${configuration['y']}"
                                    data-gs-width="${configuration['width']}" 
                                    data-gs-height="${configuration['height']}" 
                                    data-gs-min-width="${configuration['width']}" 
                                    data-gs-max-width="12" data-key="${configuration['key']}">
                            </div>`);
                $div.append($container);
                $wrapper.append($div);
            });
            if (!this.grid) {
                this.grid = GridStack.init({
                    removeTimeout: 100,
                    verticalMargin: 20,
                    horizontalMargin: 20
                });
            }
        },
        convertToPNG: function (e) {
            let self = this;
            let element = this.$el.find('#detailed-site-dashboard-wrapper')[0];
            element.scrollIntoView();
            let title = 'Site dashboard'
            showDownloadPopup('IMAGE', title, function (downloadRequestId) {
                self.$el.find('.btn').hide();
                let render = HtmlToCanvas(element, {
                    onrendered: function (canvas) {
                        canvas.toBlob(function (blob) {
                            uploadToDownloadRequest(downloadRequestId, blob, title + '.png');
                        }, 'image/png');
                    }
                });
                if (render && typeof render.then === 'function') {
                    render.then(function (canvas) {
                        canvas.toBlob(function (blob) {
                            uploadToDownloadRequest(downloadRequestId, blob, title + '.png');
                        }, 'image/png');
                    }).catch(function (err) {
                        console.error('HtmlToCanvas error:', err);
                    });
                }
                self.$el.find('.btn').show();
            });
        },
    })
});
