define(['backbone', 'shared', 'chartJs', 'jquery'], function (Backbone, Shared, ChartJs, $) {
    return Backbone.View.extend({
        id: 0,
        currentSpeciesSearchResult: [],
        siteChartData: {},
        siteId: null,
        siteName: null,
        siteDetailData: null,
        features: null,
        charts: [],
        originLegends: {},
        endemismLegends: {},
        consStatusLegends: {},
        chartColors: null,
        apiParameters: _.template(Shared.SearchURLParametersTemplate),
        months: {
            'january': 1,
            'february': 2,
            'march': 3,
            'april': 4,
            'may': 5,
            'june': 6,
            'july': 7,
            'august': 8,
            'september': 9,
            'october': 10,
            'november': 11,
            'december': 12
        },
        chartBackgroundColours: [
            '#8D2641',
            '#D7CD47',
            '#18A090',
            '#A2CE89',
            '#4E6440',
            '#525351'],
        initialize: function () {
            Shared.Dispatcher.on('siteDetail:show', this.show, this);
            Shared.Dispatcher.on('siteDetail:panelClosed', this.panelClosed, this);
            Shared.Dispatcher.on('siteDetail:updateCurrentSpeciesSearchResult', this.updateCurrentSpeciesSearchResult, this);
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
        updateCurrentSpeciesSearchResult: function (newList) {
            this.currentSpeciesSearchResult = newList;
        },
        show: function (id, name, zoomToObject, addMarker) {
            this.originLegends = {};
            this.endemismLegends = {};
            this.consStatusLegends = {};
            this.siteId = id;
            this.siteName = name;
            this.zoomToObject = zoomToObject;
            if (typeof addMarker === 'undefined' || addMarker === null) {
                this.addMarker = false;
            }
            this.parameters = filterParameters;
            this.parameters['siteId'] = id;
            filterParameters = $.extend(true, {}, this.parameters);
            this.url = '/api/location-site-detail/' + this.apiParameters(this.parameters);
            this.showDetail(name, zoomToObject)
        },
        panelClosed: function (e) {
            // function that is called when the panel closed
            if (!Shared.CurrentState.SEARCH) {
                Shared.Router.updateUrl('', false);
            } else {
                filterParameters['siteIdOpen'] = '';
            }
        },
        hideAll: function (e) {
            var className = $(e.target).attr('class');
            var target = $(e.target);
            if (className === 'search-result-title') {
                target = target.parent();
            }
            if (target.data('visibility')) {
                target.find('.filter-icon-arrow').addClass('fa-angle-down');
                target.find('.filter-icon-arrow').removeClass('fa-angle-up');
                target.nextAll().hide();
                target.data('visibility', false)
            } else {
                target.find('.filter-icon-arrow').addClass('fa-angle-up');
                target.find('.filter-icon-arrow').removeClass('fa-angle-down');
                target.nextAll().show();
                target.data('visibility', true)
            }
        },
        renderPieChart: function (data, speciesType, chartName, chartCanvas) {
            if (typeof data == 'undefined') {
                return null;
            }
            // Use chart colors from API or fallback to chartBackgroundColours
            var backgroundColours = this.chartColors || this.chartBackgroundColours;
            var chartConfig = {
                type: 'pie',
                data: {
                    datasets: [{
                        data: data[speciesType][chartName + '_chart']['data'],
                        backgroundColor: backgroundColours
                    }],
                    labels: data[speciesType][chartName + '_chart']['keys']
                },
                options: {
                    responsive: false,
                    legend: {display: false},
                    title: {display: false},
                    hover: {mode: 'nearest', intersect: false},
                    borderWidth: 0,
                }
            };
            chartCanvas = this.resetCanvas(chartCanvas);
            var ctx = chartCanvas.getContext('2d');
            new ChartJs(ctx, chartConfig);

            // Render chart labels
            var dataKeys = data[speciesType][chartName + '_chart']['keys'];
            var dataLength = dataKeys.length;
            var chart_labels = {};
            chart_labels[chartName] = '';
            for (var i = 0; i < dataLength; i++) {
                chart_labels[chartName] += '<div><span style="color:' +
                    backgroundColours[i] + ';">■</span>' +
                    '<span class="species-ssdd-legend-title">&nbsp;' +
                    dataKeys[i] + '</span></div>'
            }
            var element_name = `#rp-${chartName}-legend`;
            $(element_name).html(chart_labels[chartName]);
        },
        renderSiteDetailInfo: function (data) {
            var $detailWrapper = $('<div></div>');
            if (data.hasOwnProperty('site_detail_info')) {
                let siteDetailsTemplate = _.template($('#site-details-template').html());
                $detailWrapper.append(siteDetailsTemplate(data));
            }
            return $detailWrapper;
        },
        climateApiUrl: 'https://power.larc.nasa.gov/api/temporal/climatology/point',
        climateVariables: [
            {key: 'PRECTOTCORR', title: 'Average Monthly Rainfall', unit: '(mm/day)'},
            {key: 'T2M', title: 'Average Monthly Temperature', unit: '(°C)'}
        ],
        climateMonthKeys: ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'],
        climateMonthLabels: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
        renderClimateData: function (data, containerElement) {
            let self = this;
            if (!data.hasOwnProperty('site_detail_info')) {
                return;
            }
            let siteCoordinates = (data['site_detail_info']['site_coordinates'] || '').split(',');
            let longitude = (siteCoordinates[0] || '').trim();
            let latitude = (siteCoordinates[1] || '').trim();
            if (!longitude || !latitude || longitude === 'Unknown' || latitude === 'Unknown') {
                return;
            }
            $.ajax({
                url: this.climateApiUrl,
                method: 'GET',
                dataType: 'json',
                data: {
                    parameters: this.climateVariables.map(function (v) { return v.key; }).join(','),
                    community: 'AG',
                    longitude: longitude,
                    latitude: latitude,
                    format: 'JSON'
                },
                success: function (response) {
                    let parameters = response && response.properties && response.properties.parameter;
                    if (!parameters) {
                        return;
                    }
                    let singleClimateDataTemplate = _.template($('#climate-data-template').html());
                    self.climateVariables.forEach(function (variable) {
                        let monthlyValues = parameters[variable.key];
                        if (!monthlyValues) {
                            return;
                        }
                        let climateKey = variable.key.toLowerCase() + '-climate-chart';
                        containerElement.append(singleClimateDataTemplate({
                            'title': variable.title,
                            'key': climateKey,
                            'data': {},
                            'wrapper': climateKey + '-wrapper'
                        }));
                        self.renderMonthlyLineChart({
                            'values': self.climateMonthKeys.map(function (m) { return monthlyValues[m]; }),
                            'keys': self.climateMonthLabels
                        }, climateKey, variable.unit);
                    });
                },
                error: function () {
                    // Third-party climate API unavailable; leave the panel empty.
                }
            });
        },
        createDataSummary: function (data) {
            var bio_data = data['biodiversity_data'];
            var origin_pie_canvas = document.getElementById('fish-rp-origin-pie');
            this.renderPieChart(bio_data, 'fish', 'origin', origin_pie_canvas);

            var endemism_pie_canvas = document.getElementById('fish-rp-endemism-pie');
            this.renderPieChart(bio_data, 'fish', 'endemism', endemism_pie_canvas);

            var conservation_status_pie_canvas = document.getElementById('fish-rp-conservation-status-pie');
            this.renderPieChart(bio_data, 'fish', 'cons_status', conservation_status_pie_canvas);
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
        renderMonthlyLineChart: function (climateData, canvasId, unit) {
            let chartConfig = {
                type: 'line',
                data: {
                    datasets: [{
                        data: climateData['values'],
                        backgroundColor: '#D7CD47',
                        borderColor: '#D7CD47',
                        fill: false
                    }],
                    labels: climateData['keys']
                },
                options: {
                    responsive: true,
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
                                labelString: unit || '(mm)'
                            }
                        }]
                    }
                }
            };
            let chartCanvas = document.getElementById(canvasId);
            chartCanvas = this.resetCanvas(chartCanvas);
            let ctx = chartCanvas.getContext('2d');
            new ChartJs(ctx, chartConfig);
        },
        parseNameFromAliases: function (alias, alias_type, data) {
            var name = alias;
            var choices = [];
            var index = 0;
            if (alias_type === 'cons_status') {
                choices = this.flatten_arr(data['iucn_name_list']);
            }
            if (alias_type === 'origin') {
                choices = this.flatten_arr(data['origin_name_list']);
            }
            if (choices.length > 0) {
                index = choices.indexOf(alias) + 1;
                name = choices[index];
            }
            return name;
        },
        showDetail: function (name, zoomToObject) {
            var self = this;
            // Render basic information
            var $siteDetailWrapper = $('<div></div>');
            $siteDetailWrapper.append(
                '<div id="site-detail" class="search-results-wrapper">' +
                '<div class="search-results-total" data-visibility="false"> ' +
                '<span class="search-result-title"> Site Details </span> ' +
                '<i class="fa fa-angle-down pull-right filter-icon-arrow"></i></div></div>');
            $siteDetailWrapper.append(
                '<div id="biodiversity-data" class="search-results-wrapper">' +
                '<div class="search-results-total" data-visibility="false"> ' +
                '<span class="search-result-title"> Biodiversity Data </span> ' +
                '<i class="fa fa-angle-down pull-right filter-icon-arrow"></i></div></div>');


            // Only show climate data panel if not viewing climate module results
            let currentModule = document.querySelector('input[name="module"]:checked');
            if (!currentModule || currentModule.value !== 'climate') {
                $siteDetailWrapper.append(
                    '<div id="climate-data" class="search-results-wrapper">' +
                    '<div class="search-results-total" data-visibility="false"> ' +
                    '<span class="search-result-title"> Climate Data </span> ' +
                    '<i class="fa fa-angle-down pull-right filter-icon-arrow"></i></div></div>');
            }

            Shared.Dispatcher.trigger('sidePanel:openSidePanel', {});
            Shared.Dispatcher.trigger('sidePanel:fillSidePanelHtml', $siteDetailWrapper);
            Shared.Dispatcher.trigger('sidePanel:updateSidePanelTitle', '<i class="fa fa-map-marker"></i> Loading...');
            $siteDetailWrapper.find('.search-results-total').click(self.hideAll);
            $siteDetailWrapper.find('.search-results-total').click();

            // call detail
            if (Shared.LocationSiteDetailXHRRequest) {
                Shared.LocationSiteDetailXHRRequest.abort();
                Shared.LocationSiteDetailXHRRequest = null;
            }
            Shared.LocationSiteDetailXHRRequest = $.get({
                url: this.url,
                dataType: 'json',
                success: function (data) {
                    self.siteDetailData = data;
                    Shared.Dispatcher.trigger('sidePanel:updateSiteDetailData', self.siteDetailData);

                    if (Shared.CurrentState.SEARCH) {
                        filterParameters['siteIdOpen'] = data['id'];
                    }
                    let updatedUrl = Shared.UrlUtil.updateUrlParams(window.location.href, 'site', 'siteIdOpen', data['id']);
                    // TODO : update current url

                    if (data['geometry']) {
                        let feature = {
                            id: data['id'],
                            type: "Feature",
                            geometry: JSON.parse(data['geometry']),
                            properties: {}
                        };
                        let features = new ol.format.GeoJSON().readFeatures(feature, {
                            featureProjection: 'EPSG:3857'
                        });

                        // Show marker
                        if (zoomToObject) {
                            Shared.Dispatcher.trigger('map:switchHighlight', features, !zoomToObject);
                        } else {
                            Shared.Dispatcher.trigger('map:switchHighlight', features, true);
                        }
                    }
                    let sidePanelTitle = '<i class="fa fa-map-marker"></i> ' + data['site_detail_info']['site_code'];
                    if (isStaff || ( userID !== null && userID === data['owner']) ) {
                        sidePanelTitle += '<a href="/location-site-form/update/?id=' + data['id'] + '" style="float: right; padding-top: 5px">Edit</a>';
                    }
                    Shared.Dispatcher.trigger('sidePanel:updateSidePanelTitle', sidePanelTitle);

                    // render site detail
                    $('#site-detail').append(self.renderSiteDetailInfo(data));
                    self.renderBiodiversityDataSection($('#biodiversity-data'), data);
                    self.renderCharts();
                    self.renderLegends(self.originLegends, $('.origin-legends'));
                    self.renderLegends(self.endemismLegends, $('.endemism-legends'));
                    self.renderLegends(self.consStatusLegends, $('.cons-status-legends'));

                    // Only render climate data panel if not viewing climate module results
                    let currentModule = document.querySelector('input[name="module"]:checked');
                    if (!currentModule || currentModule.value !== 'climate') {
                        self.renderClimateData(data, $('#climate-data'));
                    }

                    Shared.LocationSiteDetailXHRRequest = null;

                    // Features other than default site data exist, show the feature info
                    let siteCoordinates = data['site_detail_info']['site_coordinates'].split(',');
                    let lon = siteCoordinates[0].trim();
                    let lat = siteCoordinates[1].trim();
                    Shared.Dispatcher.trigger('layers:showFeatureInfo', lon, lat, true);
                },
                error: function (req, err) {
                    Shared.Dispatcher.trigger('sidePanel:updateSidePanelHtml', {});
                },
            });
        },
        renderBiodiversityDataSection: function (container, data) {
            let self = this;
            let biodiversitySectionTemplate = _.template($('#biodiversity-data-template-new').html());
            let ecosystem_type = '';
            try {
                ecosystem_type = data.site_detail_info.ecosystem_type;
            } catch (e) {
            }
            container.append(biodiversitySectionTemplate({
                ecosystem_type: ecosystem_type,
                data: data.biodiversity_data,
                is_sass_enabled: is_sass_enabled,
                is_water_temperature_enabled: is_water_temperature_enabled,
                is_climate_enabled: is_climate_enabled,
                is_groundwater_enabled: typeof is_groundwater_enabled !== 'undefined' && is_groundwater_enabled,
                is_pesticide_dashboard_enabled: is_pesticide_dashboard_enabled,
                sass_exist: data.sass_exist,
                add_data: true,
                water_temperature_exist: data.water_temperature_exist,
                physico_chemical_exist: data.physico_chemical_exist,
                climate_exist: data.climate_exist,
            }));
            $.each(data['biodiversity_data'], function (key, value) {
                if (key === 'source_references') {
                    return;
                }
                self.charts.push({
                    'canvas': $("#origin-chart-" + value.module),
                    'data': value['origin'],
                    'legends': self.originLegends
                });
                self.charts.push({
                    'canvas': $("#endemism-chart-" + value.module),
                    'data': value['endemism'],
                    'legends': self.endemismLegends
                });
                self.charts.push({
                    'canvas': $("#cons-chart-" + value.module),
                    'data': value['cons_status'],
                    'legends': self.consStatusLegends
                });
            });
            $('.sp-open-dashboard').click(function (e) {
                let parameters = $.extend(true, {}, filterParameters);

                const $target = $(e.target);
                if ($target.hasClass("disabled")) {
                    return false;
                }
                parameters['modules'] = $target.data('module')
                parameters['spatialFilter'] = encodeURIComponent(parameters['spatialFilter']);
                Shared.Router.updateUrl('site-detail/' + self.apiParameters(parameters).substr(1), true);
            });
            $('.sp-add-record').click(function (e) {
                let url = '#';
                const $target = $(e.target);
                if ($target.hasClass("disabled")) {
                    return false;
                }
                const moduleId = $target.data('module-id');
                const moduleName = $target.data('module-name');
                if (moduleName.toLowerCase() === 'fish') {
                    url = '/fish-form/?siteId=' + self.siteId;
                } else if (moduleName.toLowerCase() === 'invertebrates') {
                    url = '/invert-form/?siteId=' + self.siteId;
                } else if (moduleName.toLowerCase() === 'algae') {
                    url = '/algae-form/?siteId=' + self.siteId;
                } else {
                    url = `/module-form/?siteId=${self.siteId}&module=${moduleId}`;
                }
                window.location = url;
            });
            $('.spatial-dashboard-btn').click(function () {
                let url = '/spatial-dashboard/';
                url += self.apiParameters(filterParameters);
                window.location.href = url;
            });
            $('.sp-sass-dashboard').click(function () {
                let sassUrl = '';
                if (typeof self.siteId !== 'undefined') {
                    sassUrl = '/sass/dashboard/' + self.siteId + '/';
                } else {
                    sassUrl = '/sass/dashboard-multi-sites/';
                }
                sassUrl += self.apiParameters(filterParameters);
                window.location.href = sassUrl;
            });
            $('.sp-add-sass').click(function () {
                window.location.href = '/sass/' + self.siteId;
            });
            $('.sp-add-water-temperature').click(function () {
                window.location.href = '/water-temperature-form/?siteId=' + self.siteId;
            });
            $('.sp-add-physico-chemical-data').click(function () {
                window.location.href = '/physico-chemical-form/?siteId=' + self.siteId;
            });
            $('.sp-water-temperature').click(function (e) {
                let waterTemperatureUrl = '';
                if (typeof self.siteId !== 'undefined') {
                    waterTemperatureUrl = '/water-temperature/' + self.siteId + '/';
                }
                waterTemperatureUrl += self.apiParameters(filterParameters);
                window.location.href = waterTemperatureUrl;
            });
            $('.sp-physico-chemical').click(function (e) {
                let url = '/physico-chemical/' + self.siteId + '/';
                url += self.apiParameters(filterParameters);
                window.location.href = url;
            });
            // Show climate button only when climate module is selected
            if ($('#climate-module').is(':checked')) {
                $('.climate-data-row').show();
            }
            // Show groundwater button only when groundwater module is selected
            if ($('#groundwater-module').is(':checked')) {
                $('.groundwater-data-row').show();
            }
            $('input[name="module"]').on('change', function () {
                $('.climate-data-row').toggle($(this).val() === 'climate');
                $('.groundwater-data-row').toggle($(this).val() === 'groundwater');
            });

            $('.sp-groundwater-dashboard').click(function (e) {
                window.location.href = '/groundwater/' + self.siteId + '/';
            });

            $('.sp-climate-dashboard').click(function (e) {
                let climateUrl = '';
                if (typeof self.siteId !== 'undefined') {
                    climateUrl = '/climate/' + self.siteId + '/';
                } else {
                    climateUrl = '/climate/dashboard-multi-sites/';
                }
                climateUrl += self.apiParameters(filterParameters);
                window.location.href = climateUrl;
            });
            $('.sp-pesticide-dashboard').click(function(e) {
                let url = '/pesticide-dashboard/' + self.siteId + '/';
                url += self.apiParameters(filterParameters);
                window.location.href = url;
            });
        },
        flatten_arr: function (arr) {
            let self = this;
            return arr.reduce(function (flat, toFlatten) {
                return flat.concat(Array.isArray(toFlatten) ? self.flatten_arr(toFlatten) : toFlatten);
            }, []);
        },
        renderCharts: function () {
            let self = this;
            $.each(this.charts, function (index, chart) {
                if (typeof chart['data'] !== 'undefined') {
                    if (chart['data'].length > 0) {
                        self.createPieChart(chart);
                    }
                }
            })
        },
        createPieChart: function (chartData) {
            let self = this;
            let labels = [];
            let dataset = [];
            let colours = [];
            let data = chartData['data'];
            let chartCanvas = chartData['canvas'];
            let legends = chartData['legends'];
            $.each(data, function (key, value) {
                labels.push(value['name']);
                dataset.push(value['count']);

                if(value.hasOwnProperty('colour')) {
                    colours.push(value['colour']);
                    legends[value['name']] = value['colour'];
                } else {
                    if (legends.hasOwnProperty(value['name'])) {
                        colours.push(legends[value['name']]);
                    } else {
                        let length = Object.keys(legends).length;
                        let chartColors = self.chartColors || self.chartBackgroundColours;
                        colours.push(chartColors[length]);
                        legends[value['name']] = chartColors[length];
                    }
                }
            });

            let chartConfig = {
                type: 'pie',
                data: {
                    datasets: [{
                        data: dataset,
                        backgroundColor: colours
                    }],
                    labels: labels
                },
                options: {
                    responsive: false,
                    legend: {display: false},
                    title: {display: false},
                    hover: {mode: 'nearest', intersect: false},
                    borderWidth: 0,
                }
            };
            let ctx = chartCanvas[0].getContext('2d');
            new ChartJs(ctx, chartConfig);
        },
        renderLegends: function (legends, container) {
            $.each(legends, function (key, value) {
                container.append('<div><span style="color:' +
                    value + ';">■</span>' +
                    '<span style="font-style: italic;">' +
                    key + '</span></div>');
            });
        },
    })
});
