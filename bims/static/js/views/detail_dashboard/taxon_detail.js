define(['backbone', 'shared', 'underscore', 'jquery', 'chartJs', 'fileSaver', 'htmlToCanvas'], function (
    Backbone,
    Shared,
    _,
    $,
    ChartJs,
    FileSaver,
    Html2Canvas
) {
    return Backbone.View.extend({
        id: 'detailed-taxa-dashboard',
        template: _.template($('#taxon-detail-dashboard-template').html()),
        objectDataByYear: 'object_data_by_year',
        yearsArray: 'years_array',
        isOpen: false,
        csvDownloadsBaseUrl: '/api/csv-download/',
        chartConfigs: {},
        events: {
            'click .close-dashboard': 'closeDashboard',
            'click #export-taxasite-map': 'exportTaxasiteMap',
            'click .download-taxa-records-timeline': 'downloadTaxaRecordsTimeline',
            'click .ssdd-export': 'downloadElementEvent',
            'click .download-citation': 'downloadCitationList',
            'click .download-as-csv': 'downloadAsCSV',
        },
        apiParameters: _.template(Shared.SearchURLParametersTemplate),

        initialize: function () {
            this.$el.hide();
        },
        render: function () {
            this.gbifId = null;
            this.$el.html(this.template());
            this.loadingDashboard = this.$el.find('.loading-dashboard');
            this.dashboardTitleContainer = this.$el.find('.detailed-dashboard-title');
            this.originInfoList = this.$el.find('.origin-info-list');
            this.endemicInfoList = this.$el.find('.endemic-info-list');
            this.conservationStatusList = this.$el.find('#fsdd-conservation-status-card');
            this.overviewTaxaTable = this.$el.find('.overview-taxa-table');
            this.overviewNameTaxonTable = this.$el.find('.overview-name-taxonomy-table');
            this.taxaRecordsTimelineGraph = this.$el.find('#taxa-records-timeline-graph');
            this.endemismBlockData = this.$el.find('#endemism-block-data');
            this.taxaRecordsTimelineGraphChart = null;
            this.taxaRecordsTimelineGraphCanvas = this.taxaRecordsTimelineGraph[0].getContext('2d');
            this.originBlockData = this.$el.find('#origin-block-data');
            this.recordsTable = this.$el.find('.records-table');
            this.recordsAreaTable = this.$el.find('.records-area-table');
            this.mapTaxaSite = null;
            this.csvDownloadsUrl = '';
            this.imagesCard = this.$el.find('#fsdd-images-card-body');
            this.iucnLink = this.$el.find('#fsdd-iucn-link');
            this.wormsLink = this.$el.find('#fsdd-worms-link');
            this.iucnAssessmentsBody = this.$el.find('#fsdd-iucn-assessments-body');
            this.metadataTableList = this.$el.find('#metadata-table-list-taxon');
            this.sourceReferenceIds = [];

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
                source: this.siteLayerSource
            });
            return this;
        },
        show: function (data) {
            var self = this;
            if (this.isOpen) {
                return false;
            }
            Shared.Dispatcher.trigger('bugReport:moveRight');
            this.isOpen = true;
            this.loadingDashboard.show();

            this.$el.show('slide', {
                direction: 'right'
            }, 300, function () {
                self.url = '/api/bio-collection-summary/';
                if (typeof data === 'string') {
                    self.url += '?' + data;
                    self.taxonId = data.split('&')[0].split('=')[1];
                    self.csvDownloadsUrl = self.csvDownloadsBaseUrl + '?' + data;
                } else {
                    self.taxonName = data.taxonName;
                    self.taxonId = data.taxonId;
                    self.siteDetail = data.siteDetail;
                    if (typeof filterParameters !== 'undefined') {
                        self.parameters = filterParameters;
                        self.parameters['taxon'] = self.taxonId;
                    }
                    Shared.Router.updateUrl('species-detail/' + self.apiParameters(filterParameters).substr(1), true);
                    let params = self.apiParameters(self.parameters);
                    self.csvDownloadsUrl = self.csvDownloadsBaseUrl + params;
                    self.url += params;

                }
                self.fetchRecords();
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
                    alertModalBody.html(downloadRequestMessage);
                    $('#alertModal').modal({
                        'keyboard': false,
                        'backdrop': 'static'
                    });
                    const format  = $('#download-format').val()

                    let url = self.csvDownloadsUrl;
                    if (!url.includes('?')) {
                        url += '?';
                    }
                    url += '&downloadRequestId=' + downloadRequestId + '&downloadFormat='+format
                    $.get({
                        url: url,
                        dataType: 'json',
                        success: function (data) {}
                    });
                }, false)
            }
            button.html(name);
            button.prop('disabled', false);
        },
        fetchRecords: function () {
            var self = this;
            $.get({
                url: this.url,
                dataType: 'json',
                success: function (data) {
                    self.generateDashboard(data);
                    self.renderMetadataTable(data);
                    self.loadingDashboard.hide();
                    self.renderCitesTable(data);
                }
            })
        },
        renderCitesTable: function (data) {
            const tableElement = document.getElementById('citesTable');
            const tbodyElement = tableElement.querySelector('tbody');
            const messageElement = document.getElementById('citesTableMessage');
            const spinnerElement = document.getElementById('cites-spinner');
            const citesContainer = document.querySelector('.taxon-cites-container');
            const speciesPlusLink = document.getElementById('speciesPlusLink');
            if (!is_logged_in) {
                spinnerElement.style.display = 'none';
                messageElement.style.display = 'block';
                messageElement.textContent = 'Please log in to view this data.';
                return
            }
            tableElement.style.display = 'none';
            speciesPlusLink.style.display = 'none';
            messageElement.style.display = 'none';
            spinnerElement.style.display = 'block';
            tbodyElement.innerHTML = '';

            fetch('/api/taxa-cites-status/', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': csrfmiddlewaretoken,
                },
                body: JSON.stringify({ taxon_name: data['canonical_name'] })
            })
            .then(response => response.json())
            .then(data => {
                spinnerElement.style.display = 'none';
                if (data.error) {
                    messageElement.textContent = data.error;
                    tableElement.style.display = 'none';
                    messageElement.style.display = 'block';
                } else {
                    citesContainer.appendChild(tableElement);
                    if (data.cites_listing_info && data.cites_listing_info.length > 0) {
                        data.cites_listing_info.forEach(item => {
                            const row = document.createElement('tr');
                            const appendixCell = document.createElement('td');
                            appendixCell.textContent = item.appendix;
                            const annotationCell = document.createElement('td');
                            annotationCell.textContent = item.annotation;
                            const effectiveDateCell = document.createElement('td');
                            effectiveDateCell.textContent = new Date(item.effective_at).toLocaleDateString();

                            row.appendChild(appendixCell);
                            row.appendChild(annotationCell);
                            row.appendChild(effectiveDateCell);
                            tbodyElement.appendChild(row);
                        });
                        tableElement.style.display = 'table';

                        let speciesPlusLinkElm = document.createElement('a');

                        speciesPlusLinkElm.href = `https://speciesplus.net/species#/taxon_concepts/${data['taxon_concept_id']}/legal`;
                        speciesPlusLinkElm.target = '_blank';
                        speciesPlusLinkElm.textContent = 'Species+ page';
                        speciesPlusLinkElm.style.textDecoration = 'underline';
                        speciesPlusLink.style.marginBottom = '5px';
                        speciesPlusLink.style.marginTop = '5px';
                        speciesPlusLink.style.marginLeft = '10px';
                        speciesPlusLink.innerHTML = speciesPlusLinkElm.outerHTML;
                        speciesPlusLink.style.display = 'block';
                    } else {
                        messageElement.textContent = 'No CITES listing information available.';
                        tableElement.style.display = 'none';
                        messageElement.style.display = 'block';
                    }
                }
            })
            .catch(error => {
                spinnerElement.style.display = 'none';
                messageElement.textContent = `An error occurred: ${error.message}`;
                tableElement.style.display = 'none';
                messageElement.style.display = 'block';
            });
        },
        displayTaxonomyRank: function (taxonomy_rank, accepted_taxonomy_name) {
            let taxononomyRankList = _.template($('#taxon-detail-table').html());
            this.overviewNameTaxonTable.html(taxononomyRankList({
                accepted_taxonomy_name: accepted_taxonomy_name || '',
                kingdom: taxonomy_rank['KINGDOM'] || '',
                phylum: taxonomy_rank['PHYLUM'] || '',
                my_class: taxonomy_rank['CLASS'] || '',
                order: taxonomy_rank['ORDER'] || '',
                superfamily: taxonomy_rank['SUPERFAMILY'] || '',
                family: taxonomy_rank['FAMILY'] || '',
                subfamily: taxonomy_rank['SUBFAMILY'] || '',
                tribe: taxonomy_rank['TRIBE'] || '',
                subtribe: taxonomy_rank['SUBTRIBE'] || '',
                genus: taxonomy_rank['GENUS'] || '',
                subgenus: taxonomy_rank['SUBGENUS'] || '',
                species: taxonomy_rank['SPECIES'] || '',
                subspecies: taxonomy_rank['SUBSPECIES'] || '',
                variety: taxonomy_rank['VARIETY'] || '',
            }));
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

            if (!self.sourceReferenceIds || self.sourceReferenceIds.length === 0) {
                alertModalBody.html('No source references available to download.');
                $('#alertModal').modal({'keyboard': false, 'backdrop': 'static'});
                return;
            }

            showDownloadPopup('CSV', 'Citation List', function (downloadRequestId) {
                let formData = new FormData();
                formData.append('citation_format', format);
                formData.append('download_request_id', downloadRequestId);
                $.each(self.sourceReferenceIds, function (i, id) {
                    formData.append('source_reference_ids', id);
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
        renderMetadataTable: function (data) {
            var self = this;
            this.metadataTableList.html(' ');
            let dataSources = data['source_references'];
            this.sourceReferenceIds = dataSources.map(function (s) { return s['ID']; });
            let order = ['Reference Category', 'Author/s', 'Year', 'Title', 'Source', 'DOI/URL', 'Notes'];
            let orderedDataSources = [];
            for (var j=0; j<dataSources.length; j++) {
                orderedDataSources.push({})
                for (var i = 0; i < order.length; i++) {
                    orderedDataSources[j][order[i]] = dataSources[j][order[i]];
                }
            }

            var headerDiv = $('<thead><tr></tr></thead>');
            if(orderedDataSources.length > 0) {
                var keys = Object.keys(orderedDataSources[0]);
                for (var i = 0; i < keys.length; i++) {
                    headerDiv.append('<th>' + keys[i] + '</th>')
                }
            }
            self.metadataTableList.append(headerDiv);

            var bodyDiv = $('<tbody></tbody>');
            $.each(orderedDataSources, function (index, source) {
                var itemDiv = $('<tr></tr>');
                var keys = Object.keys(source);
                var document = false;
                for(var i=0; i<keys.length; i++){
                    if(source[keys[i]] === 'Published book, report or thesis'){
                        document = true
                    }

                    if (keys[i] === 'DOI/URL' && document && source[keys[i]] && source[keys[i]].indexOf('/uploaded/') > -1) {
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
            self.metadataTableList.append(bodyDiv);
        },
        generateDashboard: function (data) {
            var self = this;
            this.dashboardTitleContainer.html(data['taxon']);
            if (data['common_name'] !== '') {
                this.dashboardTitleContainer.append('<div class="common-name-title">' + data['common_name'] + '</div>');
            }
            var taxonomy_id = data['process_id'];
            var canonicalName = data['taxon'];
            var common_name = data['common_name'];
            var iucn_redlist_id = data['iucn_id'];
            var iucn_url = data['iucn_url'];
            self.taxonName = canonicalName;

            if (iucn_url) {
                this.iucnLink.attr('href', iucn_url);
            }

            var worms_url = data['worms_url'];
            if (worms_url) {
                this.wormsLink.attr('href', worms_url).show();
            } else {
                this.wormsLink.hide();
            }


            var origin_block_data = {};
            var origin_dict = {
                'indigenous': 'Native',
                'alien': 'Non-Native',
                'translocated': 'Non-Native'
            };
            origin_block_data['keys'] = ['Native', 'Non-Native', 'Unknown'];
            origin_block_data['value'] = origin_dict[data['origin']];
            origin_block_data['value_title'] = origin_block_data['value'];
            this.originBlockData.append(self.renderFBISRPanelBlocks(origin_block_data));

            var endemism_block_data = {};
            endemism_block_data['value'] = data['endemism'];
            endemism_block_data['keys'] = Shared.EndemismList;
            endemism_block_data['value_title'] = data['endemism'];
            this.endemismBlockData.append(self.renderFBISRPanelBlocks(endemism_block_data));

            //Set conservation status
            var cons_status_block_data = {};
            cons_status_block_data['value'] = this.iucn_title_from_choices(data['conservation_status'], data);
            cons_status_block_data['keys'] = ['NE', 'DD', 'LC', 'NT', 'VU', 'EN', 'CR', 'EW', 'EX'];
            for (let i = 0; i < cons_status_block_data['keys'].length; i++) {
                let next_key = cons_status_block_data['keys'][i];
                cons_status_block_data['keys'][i] = this.iucn_title_from_choices(next_key, data);
            }
            ;
            cons_status_block_data['value_title'] = cons_status_block_data['value'];
            this.conservationStatusList.append(self.renderFBISRPanelBlocks(cons_status_block_data));
            this.renderIucnAssessments(data);

            var overViewTable = _.template($('#taxon-overview-table').html());
            this.overviewTaxaTable.html(overViewTable({
                csv_downloads_url: self.csvDownloadsUrl,
                count: data['total_records'],
                taxon_class: data['taxon'],
                col_id: data['col_id'],
                common_name: data['common_name'],
                additional_data: data['taxon_additional_data'],
                taxonomic_status: data['taxonomic_status'] || '',
                accepted_taxonomy_name: data['accepted_taxonomy_name'] || ''
            }));

            let recordsOverTimeData = data['records_over_time_data'];
            let recordsOverTimeLabels = data['records_over_time_labels'];
            var recordsOptions = {
                maintainAspectRatio: false,
                title: {display: false, text: 'Records'},
                legend: {display: false},
                scales: {
                    xAxes: [{
                        barPercentage: 0.4,
                        ticks: {
                            autoSkip: false,
                            maxRotation: 90,
                            minRotation: 90
                        },
                        scaleLabel: {display: false, labelString: 'Year'}
                    }],
                    yAxes: [{
                        stacked: true,
                        scaleLabel: {display: true, labelString: 'Occurrence'},
                        ticks: {
                            beginAtZero: true,
                            callback: function (value) {
                                if (value % 1 === 0) {
                                    return value;
                                }
                            },
                        },
                    }]
                }
            };

            var objectDatasets = [{
                data: recordsOverTimeData,
                backgroundColor: 'rgba(222, 210, 65, 1)',
                label: data['taxon']
            }];
            this.taxaRecordsTimelineGraphChart = self.createTimelineGraph(
                self.taxaRecordsTimelineGraphCanvas,
                recordsOverTimeLabels,
                objectDatasets,
                recordsOptions,
                'taxon_occurrences');
            this.displayTaxonomyRank(data['taxonomy_rank'], data['accepted_taxonomy_name']);

            if (!this.mapTaxaSite) {
                const baseLayer = [];
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
                this.mapTaxaSite = new ol.Map({
                    controls: ol.control.defaults.defaults().extend([
                        new ol.control.ScaleLine()
                    ]),
                    layers: baseLayer,
                    target: 'taxasite-map',
                    view: new ol.View({
                        center: [0, 0],
                        zoom: 2
                    })
                });
                var graticule = new ol.layer.Graticule({
                    strokeStyle: new ol.style.Stroke({
                        color: 'rgba(0,0,0,1)',
                        width: 1,
                        lineDash: [2.5, 4]
                    }),
                    showLabels: true
                });
                graticule.setMap(this.mapTaxaSite);
                this.mapTaxaSite.addLayer(this.siteTileLayer);
            }

            let newParams = {
                layers: locationSiteGeoserverLayer,
                format: 'image/png',
                viewparams: 'where:' + tenant + '."' + data['sites_raw_query'] + '"'
            };
            this.siteLayerSource.updateParams(newParams);
            this.siteLayerSource.refresh();

            // Zoom to extent
            let ext = ol.proj.transformExtent(data['extent'], ol.proj.get('EPSG:4326'), ol.proj.get('EPSG:3857'));
            this.mapTaxaSite.getView().fit(ext, this.mapTaxaSite.getSize());
            if (this.mapTaxaSite.getView().getZoom() > 8) {
                this.mapTaxaSite.getView().setZoom(8);
            }

            var $tableArea = $('<div class="container"></div>');
            $tableArea.append(`
                    <div class="row">
                    <div class="col-4 title_column">Site code</div>
                    <div class="col-4 title_column">River Name</div>
                    <div class="col-4 title_column">Occurrences</div>
                    </div>`);
            $.each(data['records_per_area'], function (index, areaRecord) {
                let site_code = areaRecord['site_code'];
                let count = areaRecord['count'];
                let river_name = areaRecord['river'];
                if (river_name === null) {
                    river_name = '-';
                }
                $tableArea.append(`
                    <div class="row">
                    <div class="col-4">${site_code}</div>
                    <div class="col-4">${river_name}</div>
                    <div class="col-4">${count}</div>
                    </div>`)
            });
            self.recordsAreaTable.html($tableArea);
            this.imagesCard.append(Shared.TaxonImagesUtil.renderTaxonImages(data['gbif_id'], self.taxonId, data['col_id']));
        },
        renderIucnAssessments: function (data) {
            var assessments = data['iucn_assessments'] || [];
            this.iucnAssessmentsBody.html('');

            if (!assessments.length) {
                this.iucnAssessmentsBody.append(
                    $('<div></div>').text('No assessment history available.')
                );
                return;
            }

            var table = $('<table class="table table-sm table-hover"></table>');
            var thead = $('<thead><tr>' +
                '<th>Year</th>' +
                '<th>Status</th>' +
                '<th>Latest</th>' +
                '<th>Scope</th>' +
                '<th>Link</th>' +
                '</tr></thead>');
            var tbody = $('<tbody></tbody>');

            for (var i = 0; i < assessments.length; i++) {
                var item = assessments[i] || {};
                var row = $('<tr></tr>');
                var status = item.red_list_category_code || '-';
                var statusLabel = status;
                if (status && data['iucn_choice_list']) {
                    statusLabel = this.iucn_title_from_choices(status, data);
                }
                row.append($('<td></td>').text(item.year_published || '-'));
                row.append($('<td></td>').text(statusLabel));
                row.append($('<td></td>').text(item.latest ? 'Yes' : 'No'));
                row.append($('<td></td>').text(item.scope_label || '-'));
                if (item.url) {
                    row.append($('<td></td>').append(
                        $('<a></a>')
                            .attr('href', item.url)
                            .attr('target', '_blank')
                            .text('View')
                    ));
                } else {
                    row.append($('<td></td>').text('-'));
                }
                tbody.append(row);
            }

            table.append(thead);
            table.append(tbody);
            this.iucnAssessmentsBody.append(table);
        },
        downloadElementEvent: function (button_el) {
            let button = $(button_el.target);
            let that = this;
            if (!button.hasClass('btn')) {
                button = button.parent();
            }
            let target = button.data('datac');
            let element = this.$el.find('#' + target);
            let title = button.data('title');
            if (!title) {
                title = $(button).parent().find('.card-header-title').html().replaceAll(' ', '').replace(/(\r\n|\n|\r)/gm, '');
            }
            let chartDownloaded = 0;
            let titles = button.data('title').split(',');
            let chartNames = [];

            try {
                chartNames = button.data('chart').split(',');
            } catch (e) {}

            for (let i = 0; i < chartNames.length; i++) {
                if (that.chartConfigs.hasOwnProperty(chartNames[i])) {
                    svgChartDownload(that.chartConfigs[chartNames[i]], titles[i]);
                    chartDownloaded += 1;
                }
            }
            if (chartNames.length > 0 && chartDownloaded === chartNames.length) {
                return;
            }
            if (element.length > 0)
                that.downloadElement(title, element);

        },
        downloadElement: function (title, element) {
            var domElement = element[0];
            if (!domElement || !document.body.contains(domElement)) {
                return;
            }
            let self = this;
            domElement.scrollIntoView();
            showDownloadPopup('TABLE', title, function (downloadRequestId) {
                // Re-check attachment after modal interaction
                if (!domElement || !document.body.contains(domElement)) {
                    alert('The element is no longer available. Please try again.');
                    return;
                }
                function uploadCanvas(canvas) {
                    canvas.toBlob(function (blob) {
                        uploadToDownloadRequest(downloadRequestId, blob, title + '.png');
                    }, 'image/png');
                }
                var options = {};
                options.onrendered = function (canvas) {
                    uploadCanvas(canvas);
                }
                var render = Html2Canvas(domElement, options);
                if (render && typeof render.then === 'function') {
                    render.then(function (canvas) {
                        uploadCanvas(canvas);
                    }).catch(function (err) {
                        console.error('Html2Canvas error:', err);
                    });
                }
            })
        },
        triggerCanvasDownload: function (title, canvas) {
            if (!canvas) {
                return;
            }
            if (canvas.toBlob && FileSaver && FileSaver.saveAs) {
                canvas.toBlob(function (blob) {
                    FileSaver.saveAs(blob, title + '.png');
                });
                return;
            }
            var link = document.createElement('a');
            link.href = canvas.toDataURL("image/png").replace("image/png", "image/octet-stream");
            link.download = title + '.png';
            document.body.appendChild(link);
            link.click();
            link.remove();
        },
        clearDashboard: function () {
            $.each(this.conservationStatusList.children(), function (key, data) {
                var $conservationStatusItem = $(data);
                $conservationStatusItem.css('background-color', '');
            });
            $.each(this.originInfoList.children(), function (key, data) {
                var $originInfoItem = $(data);
                $originInfoItem.css('background-color', '');
            });

            this.conservationStatusList.html('');

            $.each(this.endemicInfoList.children(), function (key, data) {
                var $endemicInfoItem = $(data);
                $endemicInfoItem.css('background-color', '');
            });
            this.overviewTaxaTable.html('');
            this.overviewNameTaxonTable.html('');
            this.endemismBlockData.html('');
            this.imagesCard.html('');
            this.originBlockData.html('');
            this.metadataTableList.html('');
            // Clear canvas
            if (this.taxaRecordsTimelineGraphChart) {
                this.taxaRecordsTimelineGraphChart.destroy();
                this.taxaRecordsTimelineGraphChart = null;
            }


            if (this.mapTaxaSite) {
                let newParams = {
                    layers: locationSiteGeoserverLayer,
                    format: 'image/png',
                    viewparams: 'where:' + emptyWMSSiteParameter
                };
                this.siteLayerSource.updateParams(newParams);
            }
            this.recordsTable.html('');
            this.recordsAreaTable.html('');
        },
        closeDashboard: function () {
            var self = this;
            if (!this.isOpen) {
                return false;
            }
            Shared.Dispatcher.trigger('bugReport:moveLeft');
            this.$el.hide('slide', {
                direction: 'right'
            }, 300, function () {
                self.isOpen = false;
                self.clearDashboard();
                self.loadingDashboard.hide();
                self.dashboardTitleContainer.html('&nbsp;');
                filterParameters['taxon'] = '';
                let currentUrl = window.location.href;
                let newUrl = currentUrl.replace(/#species-detail\//, '#search//')
                    .replace(/taxon=[^&]*/, 'taxon=')
                Shared.Router.navigate(newUrl.split('#').at(-1), { trigger: false, replace: true });
            });
        },
        createTimelineGraph: function (canvas, labels, dataset, options, chartTitle = '') {
            let chartConfig = {
                type: 'bar',
                data: {
                    labels: labels,
                    datasets: dataset
                },
                options: options
            };
            var chart = new ChartJs(canvas, chartConfig)
            this.chartConfigs[chartTitle] = chartConfig;

            return chart;
        },
        exportTaxasiteMap: function () {
            let self = this;
            this.mapTaxaSite.once('postcompose', function (event) {
                showDownloadPopup('IMAGE', 'Taxa Map', function (downloadRequestId) {
                    let canvas = $('#taxasite-map')[0];
                    if (!canvas || !document.body.contains(canvas)) {
                        return;
                    }
                    function uploadMap(renderedCanvas) {
                        $('.ol-control').show();
                        renderedCanvas.toBlob(function (blob) {
                            uploadToDownloadRequest(downloadRequestId, blob, 'Taxa Map.png');
                        }, 'image/png');
                    }
                    var options = {
                        useCORS: false,
                        background: '#FFFFFF',
                        allowTaint: true,
                    };
                    options.onrendered = uploadMap;
                    var render = Html2Canvas(canvas, options);
                    if (render && typeof render.then === 'function') {
                        render.then(uploadMap);
                    }
                })
            });
            this.mapTaxaSite.renderSync();
        },
        downloadTaxaRecordsTimeline: function () {
            var title = 'taxa-record-timeline';
            var canvas = this.taxaRecordsTimelineGraph[0];
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
                    if (!canvas || !document.body.contains(canvas)) {
                        return;
                    }
                    function uploadCanvas(renderedCanvas) {
                        renderedCanvas.toBlob(function (blob) {
                            uploadToDownloadRequest(downloadRequestId, blob, title + '.png');
                        }, 'image/png');
                    }
                    var options = {};
                    options.onrendered = uploadCanvas;
                    var render = Html2Canvas(canvas, options);
                    if (render && typeof render.then === 'function') {
                        render.then(uploadCanvas);
                    }
                }
            });
        },
        renderFBISRPanelBlocks: function (data, stretch_selection = false) {
            var $detailWrapper = $('<div style="padding-left: 0;"></div>');
            $detailWrapper.append(this.getHtmlForFBISBlocks(data, stretch_selection));
            return $detailWrapper;
        },
        getHtmlForFBISBlocks: function (new_data_in, stretch_selection) {
            var result_html = '<div class ="fbis-data-flex-block-row">'
            var data_in = new_data_in;
            var data_value = data_in.value;
            var data_title = data_in.value_title;
            var keys = data_in.keys;
            var key = '';
            var style_class = '';
            var for_count = 0;
            for (let key of keys) {
                for_count += 1;
                style_class = "fbis-rpanel-block fbis-rpanel-block-dd ";
                var temp_key = key;
                //Highlight my selected box with a different colour
                if (key === data_value || (!data_value && key === 'Unknown')) {
                    style_class += " fbis-rpanel-block-selected";
                    if(key === data_value) {
                        temp_key = data_title;
                    }
                    if (stretch_selection == true) {
                        style_class += " flex-base-auto";
                    }
                }
                result_html += (`<div class="${style_class}">
                                 <div class="fbis-rpanel-block-text">
                                 ${temp_key}</div></div>`)

            }
            result_html += '</div>';
            return result_html;
        },
        origin_title_from_choices: function (short_name, origin_dict) {
            var name = short_name;
            var choices = [];
            choices = this.flatten_arr(origin_dict);
            if (origin_dict.length > 0) {
                let index = choices.indexOf(short_name) + 1;
                let long_name = choices[index];
                name = long_name;
            }
            return name;
        },
        iucn_title_from_choices: function (short_name, data) {
            var name = short_name;
            var choices = [];
            choices = this.flatten_arr(data['iucn_choice_list']);
            if (choices.length > 0) {
                let index = choices.indexOf(short_name) + 1;
                let long_name = choices[index];
                name = `${long_name} (${short_name})`;
            }
            return name;
        },
        flatten_arr: function (arr) {
            let self = this;
            return arr.reduce(function (flat, toFlatten) {
                return flat.concat(Array.isArray(toFlatten) ? self.flatten_arr(toFlatten) : toFlatten);
            }, []);
        },
    })
});
