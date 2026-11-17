define([
    'jquery',
    'backbone',
    'models/search_result',
    'views/search_result',
    'shared'], function ($, Backbone, SearchModel, SearchResultView, Shared) {
    return Backbone.Collection.extend({
        model: SearchModel,
        url: "",
        searchUrl: "/api/collection-search/",
        siteResultUrl: "/api/site-search-result/",
        taxaResultUrl: "/api/taxa-search-result/",
        viewCollection: [],
        searchPanel: null,
        searchValue: '',
        status: '',
        isFuzzySearch: false,
        initialSearch: true,
        secondSearch: false,
        searchFinish: false,
        sitesData: [],
        recordsData: [],
        totalRecords: 0,
        totalSites: 0,
        totalTaxa: 0,
        processID: 0,
        pageMoreSites: 2,
        pageMoreTaxa: 2,
        extent: [],
        searchXHR: null,
        module: 'occurrence',
        searchFinishedCallback: null,
        initialize: function (searchFinishedCallback) {
            this.searchFinishedCallback = searchFinishedCallback;
        },
        modelId: function (attrs) {
            return attrs.record_type + "-" + attrs.id;
        },
        search: function (searchPanel, parameters, shouldUpdateUrl) {
            var self = this;
            this.totalRecords = 0;
            this.totalSites = 0;
            this.totalTaxa = 0;
            this.searchValue = parameters['search'];
            this.collectorValue = parameters['collector'];
            this.categoryValue = parameters['category'];
            this.yearFrom = parameters['yearFrom'];
            this.yearTo = parameters['yearTo'];
            this.months = parameters['months'];
            this.boundary = parameters['boundary'];
            this.userBoundary = parameters['userBoundary'];
            this.referenceCategory = parameters['referenceCategory'];
            this.endemic = parameters['endemic'];
            this.reference = parameters['reference'];
            this.conservationStatus = parameters['conservationStatus'];
            this.riverCatchment = parameters['spatialFilter'];
            this.validated = parameters['validated'];
            parameters['taxon'] = '';
            parameters['siteId'] = '';

            var templateUrl = _.template(Shared.SearchURLParametersTemplate);
            this.filters = templateUrl(parameters);
            this.url = this.searchUrl + this.filters;

            // Update permalink
            if (shouldUpdateUrl) {
                var linkUrl = 'search/';
                linkUrl += this.searchValue;
                linkUrl += '/' + this.filters.substring(1, this.filters.length);
                Shared.Router.updateUrl(linkUrl);
            }

            this.searchPanel = searchPanel;
            this.searchPanel.showSearchLoading();
            this.status = '';
            this.getSearchResults();
        },
        clearSearchRequest: function () {
            if (!this.searchXHR) {
                return false;
            }
            this.status = 'abort';
            this.searchXHR.abort();
            this.searchXHR = null;
        },
        hideAll: function (e) {
            let $target = $(e.target);
            if (!$target.hasClass('search-results-total')) {
                $target = $target.parent();
            }
            if ($target.data('visibility')) {
                $target.find('.filter-icon-arrow').addClass('fa-angle-down');
                $target.find('.filter-icon-arrow').removeClass('fa-angle-up');
                $target.nextAll().hide();
                $target.data('visibility', false)
            } else {
                $target.find('.filter-icon-arrow').removeClass('fa-angle-down');
                $target.find('.filter-icon-arrow').addClass('fa-angle-up');
                $target.nextAll().show();
                $target.data('visibility', true)
            }
        },
        getSearchResults: function () {
            let self = this;
            if (this.searchXHR) {
                this.searchXHR.abort();
                this.searchXHR = null;
            }
            if (this.status === 'abort') {
                return false;
            }
            this.searchXHR = this.fetch({
                success: function () {
                    Shared.CurrentState.SEARCH = true;
                },
                complete: function () {
                    let timeout = 2000;

                    // initial search should have a small timeout
                    if (self.initialSearch) {
                        timeout = 500;
                        self.initialSearch = false;
                        self.secondSearch = true;
                    }

                    // post initial search can have bigger timeout
                    if (self.secondSearch) {
                        timeout = 1000;
                        self.secondSearch = true;
                    }

                    if (self.status === 'processing') {
                        setTimeout(function () {
                            self.getSearchResults()
                        }, timeout);
                    }
                }
            });
            return this.searchXHR;
        },
        parse: function (response) {
            this.module = document.querySelector('input[name="module"]:checked').value;
            if (response.hasOwnProperty('records')) {
                this.recordsData = response['records'];
            }
            if (response.hasOwnProperty('sites')) {
                this.sitesData = response['sites'];
            }
            if (response.hasOwnProperty('fuzzy_search')) {
                this.isFuzzySearch = response['fuzzy_search'];
            }
            if (response.hasOwnProperty('status')) {
                this.status = response['status'];
            }
            if (response.hasOwnProperty('total_records')) {
                this.totalRecords = response['total_records'];
            }
            if (this.module !== 'occurrence') {
                if (response['total']) {
                    this.totalRecords = response['total'];
                }
            }
            if (response.hasOwnProperty('total_sites')) {
                this.totalSites = response['total_sites'];
            }
            if (response.hasOwnProperty('total_unique_taxa')) {
                this.totalTaxa = response['total_unique_taxa'];
            }
            if (response.hasOwnProperty('sites_raw_query')) {
                this.sitesRawQuery = response['sites_raw_query'];
            }
            if (response.hasOwnProperty('process_id')) {
                this.processID = response['process_id'];
            }
            if (response.hasOwnProperty('extent')) {
                this.extent = response['extent'];
            }
            this.renderCollection();
        },
        renderCollection: function () {
            var self = this;
            var searchResultTitleDiv = $('<div>');
            searchResultTitleDiv.addClass('search-result-title-panel');
            searchResultTitleDiv.html(this.searchValue);
            if (this.isFuzzySearch) {
                searchResultTitleDiv.html('similar to ' + this.searchValue)
            }

            let totalSearchResults = $('<div>');
            totalSearchResults.addClass('total-search-results');
            totalSearchResults.html(numberWithCommas(this.totalRecords) + ' records');

            if (self.status === 'finished' && self.extent && self.extent.length === 4) {
                let capturedExtent = self.extent.slice();
                let zoomBtn = $('<button type="button" class="zoom-to-search-extent btn btn-sm btn-primary d-inline-flex align-items-center badge-button"><i class="fa fa-expand"></i></button>');
                zoomBtn.on('click', function() {
                    Shared.Dispatcher.trigger('map:forceZoomToExtent', capturedExtent, true, false);
                });
                zoomBtn.popover({
                    trigger: 'hover focus',
                    placement: 'top',
                    container: 'body',
                    content: 'Fit to map'
                });
                totalSearchResults.append(zoomBtn);
            }

            var searchResultHeader = $('<div>');
            searchResultHeader.append(searchResultTitleDiv);
            searchResultHeader.append(totalSearchResults);

            this.searchPanel.updatesearchPanelTitle(searchResultHeader);
            if (this.models.length === 1) {
                if (this.models[0]['attributes'].hasOwnProperty('results')) {
                    self.searchPanel.fillPanelHtml(this.models[0]['attributes']['results']);
                    return false;
                }
            }

            let $searchResultsWrapper = $('<div></div>');
            $searchResultsWrapper.append(
                '<div class="search-results-wrapper">' +
                `<div class="search-results-total" data-visibility="true"> ${locationSiteNamePlural.toUpperCase()} ` +
                '(<span id="site-list-number"></span>) ' +
                '<i class="fa fa-angle-down pull-right filter-icon-arrow"></i> <span class="site-detail-dashboard-button-wrapper"></span></div>' +
                '<div id="site-list" class="search-results-section"></div>' +
                '</div>');
            if (this.module === 'occurrence') {
                $searchResultsWrapper.append(
                    '<div class="search-results-wrapper">' +
                    '<div class="search-results-total" data-visibility="true"> TAXA ' +
                    '(<span id="taxa-list-number"></span>) ' +
                    '<i class="fa fa-angle-down pull-right filter-icon-arrow"></i> <span class="taxa-detail-dashboard-button-wrapper"></span></div>' +
                    '<div id="taxa-list" class="search-results-section"></div>' +
                    '</div>');
            }

            self.searchPanel.fillPanelHtml($searchResultsWrapper);

            $.each(this.viewCollection, function (index, view) {
                view.destroy();
            });
            this.viewCollection = [];

            let recordsCount = numberWithCommas(this.totalRecords);
            let siteCount = numberWithCommas(this.totalSites);
            let taxaCount = numberWithCommas(this.totalTaxa);
            let speciesListName = [];

            if (self.status === 'finished' && (this.sitesData.length > 0 || this.recordsData.length > 0)) {
                if (this.searchFinishedCallback) {
                    this.searchFinishedCallback();
                }
                Shared.Dispatcher.trigger('map:updateBiodiversityLayerParams', this.sitesRawQuery);
                if (this.extent.length === 4) {
                    Shared.Dispatcher.trigger('map:zoomToExtent', this.extent, true, false);
                }

                $.each(this.recordsData, function (key, data) {
                    var searchModel = new SearchModel({
                        id: data['taxon_id'],
                        count: numberWithCommas(data['total']),
                        survey: numberWithCommas(data['total_survey']),
                        name: data['name'],
                        highlight: data['name'],
                        record_type: 'taxa'
                    });
                    var searchResultView = new SearchResultView({
                        model: searchModel
                    });
                    self.viewCollection.push(searchResultView);
                    speciesListName.push(searchResultView.model.get('name'));
                });
                $.each(this.sitesData, function (key, data) {
                    let total_water_temperature_data = 0;
                    let total_chemical_records = 0;
                    let total_climate_records = 0;
                    let total_groundwater_records = 0;
                    let _total = 0;
                    let _total_survey = 0;
                    if (data['total_water_temperature_data']) {
                        total_water_temperature_data = data['total_water_temperature_data']
                    }
                    if (data['total_chemical_records']) {
                        total_chemical_records = data['total_chemical_records']
                    }
                    if (data['total_climate_records']) {
                        total_climate_records = data['total_climate_records']
                    }
                    if (data['total_groundwater_records']) {
                        total_groundwater_records = data['total_groundwater_records']
                    }
                    if (typeof data['total'] !== 'undefined') {
                        _total = data['total'];
                    }
                    if (typeof data['total_survey'] !== 'undefined') {
                        _total_survey = data['total_survey']
                    }
                    let searchModel = new SearchModel({
                        id: data['site_id'],
                        count: numberWithCommas(_total),
                        survey: numberWithCommas(_total_survey),
                        total_thermal: numberWithCommas(total_water_temperature_data),
                        total_chemical_records: numberWithCommas(total_chemical_records),
                        total_climate_records: numberWithCommas(total_climate_records),
                        total_groundwater_records: numberWithCommas(total_groundwater_records),
                        name: data['name'],
                        record_type: 'site',
                        module: self.module
                    });
                    let searchResultView = new SearchResultView({
                        model: searchModel
                    });
                    self.viewCollection.push(searchResultView);
                });

                // Set multiple site dashboard url
                const $dashboardButton = $(`
                  <button type="button"
                          class="btn btn-sm btn-primary d-inline-flex align-items-center site-dashboard-btn badge-button"
                          title="Open sites overview" aria-label="Open sites overview">
                    <span class="ms-1 d-none d-sm-inline">Sites overview >></span>
                  </button>
                `);
                $searchResultsWrapper.find('.site-detail-dashboard-button-wrapper').append($dashboardButton);
                if (this.sitesData.length > 1) {
                    $dashboardButton.click(function () {
                        Shared.Dispatcher.trigger('multiSiteDetailPanel:show');
                    });
                } else {
                    let siteId = this.sitesData[0]['site_id'];
                    let siteName = this.sitesData[0]['name'];
                    $dashboardButton.click(function () {
                        Shared.Dispatcher.trigger('siteDetail:show', siteId, siteName);
                    });
                }

                if (this.recordsData.length === 1) {
                    const $taxaDashboardButton = $(`
                      <button type="button"
                              class="btn btn-sm btn-primary d-inline-flex align-items-center site-dashboard-btn badge-button"
                              title="Open taxon overview" aria-label="Open taxon overview">
                        <span class="ms-1 d-none d-sm-inline">Taxon overview >></span>
                      </button>
                    `);
                    $searchResultsWrapper.find('.taxa-detail-dashboard-button-wrapper').append($taxaDashboardButton);
                    $taxaDashboardButton.click(function () {
                        Shared.Dispatcher.trigger(
                            'taxonDetail:show', self.recordsData[0]['taxon_id'], self.recordsData[0]['name'], null);
                    });

                }
            } else {
                if (self.status === 'finished' && (this.sitesData.length === 0 || this.recordsData.length === 0)) {
                    Shared.Dispatcher.trigger('map:clearAllLayers');
                }
            }

            var taxaListNumberElm = $('#taxa-list-number');
            var siteListNumberElm = $('#site-list-number');

            $searchResultsWrapper.find('.search-results-total').click(self.hideAll);
            $searchResultsWrapper.find('.search-results-total').click();
            $searchResultsWrapper.find('.search-results-total').unbind();

            if (self.status === 'processing') {
                taxaCount += ' ...loading';
                siteCount += ' ...loading';
            } else if (self.status === 'finished') {
                $searchResultsWrapper.find('.search-results-total').click(self.hideAll);
            }
            taxaListNumberElm.html(taxaCount);
            siteListNumberElm.html(siteCount);

            // Add show more button for site list
            if (self.sitesData.length < self.totalSites) {
                self.viewCollection.push(new SearchResultView({
                    model: new SearchModel({
                        name: 'Show More',
                        record_type: 'show-more-site'
                    })
                }));
            }

            // Add show more button for taxa list
            if (self.recordsData.length < self.totalTaxa) {
                self.viewCollection.push(
                    new SearchResultView({
                        model: new SearchModel({
                            name: 'Show More',
                            record_type: 'show-more-taxa'
                        })
                    })
                )
            }

            Shared.Dispatcher.trigger('siteDetail:updateCurrentSpeciesSearchResult', speciesListName);
        },
        clearPagination: function () {
            this.pageMoreTaxa = 2;
            this.pageMoreSites = 2;
        },
        fetchMoreTaxa: function () {
            var self = this;
            var siteResultUrl = this.taxaResultUrl + '?process_id=' + this.processID + '&page=' + this.pageMoreTaxa;
            $.ajax({
                url: siteResultUrl,
                success: function (data) {
                    var taxaData = data['data'];
                    for (var i = 0; i < taxaData.length; i++) {
                        var searchModel = new SearchModel({
                            id: taxaData[i]['taxon_id'],
                            name: taxaData[i]['name'],
                            highlight: taxaData[i]['name'],
                            record_type: 'taxa',
                            count: numberWithCommas(taxaData[i]['total']),
                            survey: numberWithCommas(taxaData[i]['total_survey']),
                        });
                        var searchResultView = new SearchResultView({
                            model: searchModel
                        });
                        self.viewCollection.push(searchResultView);
                    }
                    if (data['has_next']) {
                        self.viewCollection.push(new SearchResultView({
                            model: new SearchModel({
                            name: 'Show More',
                            record_type: 'show-more-taxa'
                            })
                        }));
                        self.pageMoreTaxa += 1;
                    } else {
                        self.pageMoreTaxa = 2
                    }
                }
            })
        },
        fetchMoreSites: function (page) {
            var self = this;
            var siteResultUrl = this.siteResultUrl + '?process_id=' + this.processID + '&page=' + this.pageMoreSites;
            $.ajax({
                url: siteResultUrl,
                success: function (data) {
                    var siteData = data['data'];
                    for (var i = 0; i < siteData.length; i++) {
                        let total_water_temperature_data = 0;
                        let total_chemical_records = 0;
                        let total_climate_records = 0;
                        let total_groundwater_records = 0;
                        if (siteData[i]['total_water_temperature_data']) {
                            total_water_temperature_data = siteData[i]['total_water_temperature_data']
                        }
                        if (siteData[i]['total_chemical_records']) {
                            total_chemical_records = siteData[i]['total_chemical_records']
                        }
                        if (siteData[i]['total_climate_records']) {
                            total_climate_records = siteData[i]['total_climate_records']
                        }
                        if (siteData[i]['total_groundwater_records']) {
                            total_groundwater_records = siteData[i]['total_groundwater_records']
                        }
                        let searchModel = new SearchModel({
                            id: siteData[i]['site_id'],
                            name: siteData[i]['name'],
                            record_type: 'site',
                            count: siteData[i]['total'] ? numberWithCommas(siteData[i]['total']) : 0,
                            survey: siteData[i]['total_survey'] ? numberWithCommas(siteData[i]['total_survey']) : 0,
                            total_thermal: numberWithCommas(total_water_temperature_data),
                            total_chemical_records: numberWithCommas(total_chemical_records),
                            total_climate_records: numberWithCommas(total_climate_records),
                            total_groundwater_records: numberWithCommas(total_groundwater_records),
                            module: self.module
                        });
                        var searchResultView = new SearchResultView({
                            model: searchModel
                        });
                        self.viewCollection.push(searchResultView);
                    }
                    if (data['has_next']) {
                        let showMoreButton = new SearchModel({
                            name: 'Show More',
                            record_type: 'show-more-site'
                        });
                        let showMoreSiteView = new SearchResultView({
                            model: showMoreButton
                        });
                        self.viewCollection.push(showMoreSiteView);
                        self.pageMoreSites += 1;
                    } else {
                        self.pageMoreSites = 2
                    }
                }
            })
        }
    })
});
