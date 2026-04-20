define([
    'backbone',
    'underscore',
    'shared',
    'noUiSlider',
    'collections/search_result',
    'views/search_panel',
    'jquery',
    'chosen',
    'views/filter_panel/reference_category',
    'views/filter_panel/spatial_filter',
    'views/filter_panel/source_collection',
    'views/filter_panel/taxon_tags',
    'views/filter_panel/gbif_dataset',
    'views/filter_panel/advanced_spatial_filter'
], function (Backbone, _, Shared, NoUiSlider, SearchResultCollection, SearchPanelView, $, Chosen,
             ReferenceCategoryView, SpatialFilterView, SourceCollectionView, TaxonTagsView, GbifDatasetView,
             AdvancedSpatialFilterView) {

    return Backbone.View.extend({
        template: _.template($('#map-search-container').html()),
        searchBox: null,
        searchBoxOpen: false,
        shouldUpdateUrl: true,
        searchResults: {},
        selectedEcologicalConditions: [],
        filtersReady: {
            'endemism': false,
            'invasions': false,
            'collector': false,
            'study-reference': false,
            'referenceCategory': false,
            'gbifDataset': false
        },
        initialSelectedCategory: [],
        initialSelectedStudyReference: [],
        initialSelectedCollectors: [],
        initialSelectedDST: [],
        initialSelectedReferenceCategory: [],
        initialSelectedSourceCollection: [],
        initialSelectedGbifDatasets: [],
        initialSelectedEndemic: [],
        initialSelectedInvasions: [],
        initialSelectedModules: [],
        currentSort: null,
        defaultSort: 'name',
        events: {
            'keyup #search': 'checkSearch',
            'keypress #search': 'searchEnter',
            'click .search-arrow': 'searchClick',
            'click .apply-filter': 'searchClick',
            'click .clear-filter-biodiversity-module': 'clearFilterBiodiversityModule',
            'click .clear-filter-module': 'clearFilterModule',
            'click .clear-filter': 'clearFilter',
            'click .search-reset': 'clearSearch',
            'click .origin-btn': 'handleOriginBtnClick',
            'click .endemic-dropdown-item': 'handleEndemicDropdown',
            'click .invasive-dropdown-item': 'handleInvasiveDropdown',
            'click .clear-origin-filter': 'handleClearOriginClicked',
            'click .clear-conservation-filter': 'handleClearConservationClicked',
            'click .ecological-condition': 'handleEcologicalConditionClicked',
            'change #sortby-select': 'handleSortByChanged',
            'change .radio-module': 'handleModuleChanged',
        },
        initialize: function (options) {
            _.bindAll(this, 'render');
            this.parent = options.parent;
            this.sidePanel = options.sidePanel;
            this.searchPanel = new SearchPanelView();
            this.lassoPanel = this.parent.lassoPanelView;
            this.searchResultCollection = new SearchResultCollection(
                this.searchFinished
            );
            this.lassoPanel.searchResultCollection = this.searchResultCollection;
            Shared.Dispatcher.on('search:searchCollection', this.search, this);
            Shared.Dispatcher.on('search:doSearch', this.searchClick, this);
            Shared.Dispatcher.on('search:clearSearch', this.clearSearch, this);
            Shared.Dispatcher.on('search:checkSearchCollection', this.checkSearch, this);
            Shared.Dispatcher.on('filters:updateFilters', this.filtersUpdated, this);
            Shared.Dispatcher.on('search:showMoreSites', this.showMoreSites, this);
            Shared.Dispatcher.on('search:showMoreTaxa', this.showMoreTaxa, this);
        },
        handleModuleChanged: function (e) {
            let value = document.querySelector('input[name="module"]:checked').value;
            if (!window.selectedModule) {
                window.selectedModule = value
            } else {
                if (window.selectedModule !== value) {
                    window.selectedModule = value;
                    $('.search-reset').click();
                    if (value === 'occurrence') {
                        $('#sortby-select').val("name").change();
                    }
                }
            }
            let occurrencesFilter = [
                'biodiversity-module-container',
                'data-source-container',
                'taxon-tags-container',
                'validation-status-container',
                'ecological-category-container',
                'origin-and-endemism-container',
                'conservation-status-container',
                'decision-support-tool-container'
            ]
            if (value === 'occurrence') {
                $('.occurrence-sort').show();
                for (let container of occurrencesFilter) {
                    if (document.getElementById(container)) {
                        document.getElementById(container).style.display = 'block';
                    }
                }
            } else {
                $('#sortby-select').val("name").change();
                for (let container of occurrencesFilter) {
                    if (document.getElementById(container)) {
                        document.getElementById(container).style.display = 'none';
                    }
                }
            }
        },
        render: function () {
            var self = this;
            this.$el.html(this.template());
            this.searchBox = this.$el.find('.map-search-box');
            this.searchInput = this.$el.find('#search');
            this.searchInput.autocomplete({
                autoFocus: true,
                source: function (request, response) {
                    var sourceCollection = filterParameters['sourceCollection'];
                    $.ajax({
                        url: "/autocomplete/",
                        data: {q: request.term, source_collection: sourceCollection},
                        dataType: "json",
                        success: function (requestResponse) {
                            var responseData = [];
                            if (requestResponse.hasOwnProperty('results')) {
                                $.each(requestResponse['results'], function (index, value) {
                                    responseData.push({
                                        'value': value['suggested_name'],
                                        'label': value['suggested_name'],
                                        'source': value['source'],
                                        'id': value['id']
                                    })
                                })
                            }
                            response(responseData);
                        }
                    })
                },
                select: function (event, ui) {
                    var itemValue = ui['item']['value'];
                    var source = ui['item']['source'];
                    // Check if source contain taxonomy rank
                    if (source.toLowerCase().includes('taxonomy rank')) {
                        let sources = source.split(':');
                        if (sources.length > 1) {
                            filterParameters['rank'] = sources[1].trim();
                        }
                    } else {
                        filterParameters['rank'] = '';
                    }
                    self.search(itemValue);
                }
            }).autocomplete("instance")._renderItem = function( ul, item ) {
                return $( "<li>" )
                    .append( "<div class='autocomplete-item'><span class='autocomplete-label'>" + item.label + "</span><hr style=\"height:0; visibility:hidden;margin-top: -5px;margin-bottom: -5px\" /><span class='autocomplete-source'>" + item.source + "</span></div>" )
                    .appendTo( ul );
            };
            this.searchBox.hide();
            this.$el.append(this.searchPanel.render().$el);
            this.referenceCategoryView = new ReferenceCategoryView({parent: this});
            this.$el.find('.reference-category-wrapper').append(this.referenceCategoryView.render().$el);

            this.advancedSpatialFilterView = new AdvancedSpatialFilterView({parent: this});
            this.spatialFilterView = new SpatialFilterView({
                advancedSpatialFilterView: this.advancedSpatialFilterView
            });
            this.$el.find('.spatial-filter-wrapper').append(this.spatialFilterView.render().$el);

            this.sourceCollectionView = new SourceCollectionView({parent: this});
            this.$el.find('.source-collection-wrapper').append(this.sourceCollectionView.render().$el);

            this.gbifDatasetView = new GbifDatasetView({parent: this});
            this.gbifDatasetView.render();
            // Pass reference to source collection view so it can append after rendering
            this.sourceCollectionView.gbifDatasetView = this.gbifDatasetView;

            this.taxonTagsView = new TaxonTagsView({parent: this});
            this.$el.find('.taxon-tags-wrapper').append(this.taxonTagsView.render().$el);

            var nativeOriginDropdown = self.$el.find('.native-origin-dropdown');
            var nonNativeOriginDropdown = self.$el.find('.non-native-origin-dropdown');
            var moduleListContainer = self.$el.find('.module-filters');

            filterParameters['orderBy'] = this.currentSort;

            $.ajax({
                type: 'GET',
                url: '/api/module-list/',
                dataType: 'json',
                success: function (data) {
                    for (var i = 0; i < data.length; i++) {
                        let selected = '';
                        if (data[i]['name'].toLowerCase() === 'fish') {
                            Shared.FishModuleID = data[i]['id'];
                        }
                        if ($.inArray(data[i]['id'].toString(), self.initialSelectedModules) > -1) {
                            selected = 'selected';
                        }
                        let $moduleSpecies = $(
                            '<div data-id="' + data[i]['id'] + '" class="col-lg-4 module-species ' + selected + '" title="' + data[i]['name'] + '">' +
                            '<img src="/uploaded/' + data[i]['logo'] + '"></div>'
                        );
                        moduleListContainer.append($moduleSpecies);
                        $moduleSpecies.click(self.onModuleSpeciesClicked);
                    }
                }
            });

            $.ajax({
                type: 'GET',
                url: '/api/endemism-list/',
                dataType: 'json',
                success: function (data) {

                    const endemismList = [];
                    for (var i = 0; i < data.length; i++) {
                        var checked = '';
                        if ($.inArray(data[i][0], self.initialSelectedEndemic) > -1) {
                            checked = 'checked';
                        }
                        let endemicLabel = data[i][0].replace(/ *\([^)]*\) */g, "");
                        var liElement = $('<li class="endemic-dropdown-item" data-endemic-value="' + data[i][0] + '">' +
                            ' <input class="endemic-checkbox" name="endemic-value" type="checkbox" value="' + data[i][0] + '" ' + checked + '> <span>' + endemicLabel + ' </span></li>');
                        if (!data[i][1]) {
                            continue;
                        }
                        nativeOriginDropdown.append(liElement);
                        liElement.popover({
                            content: data[i][1],
                            trigger: 'hover',
                            placement: 'top'
                        });
                        endemismList.push(data[i][0])
                    }
                    Shared.EndemismList = endemismList;
                    self.filtersReady['endemism'] = true;
                }
            });

            $.ajax({
                type: 'GET',
                url: '/api/invasions-list/',
                dataType: 'json',
                success: function (data) {

                    const invasiveList = [];
                    for (let i = 0; i < data.length; i++) {
                        let checked = '';
                        if ($.inArray(data[i][0] + '', self.initialSelectedInvasions) > -1) {
                            checked = 'checked';
                        }
                        let invasiveLabel = data[i][1].replace(/ *\([^)]*\) */g, "");
                        let liElement = $(`<div class="dropdown-item invasive-dropdown-item" style="padding-left: 10px; cursor: pointer" data-invasive-value="${data[i][0]}">` +
                            `<input class="invasive-checkbox" name="invasive-value" type="checkbox" value="${data[i][0]}" ${checked}>` +
                            `<span> ${invasiveLabel} </span></div>`);
                        nonNativeOriginDropdown.append(liElement);
                        if (data[i][2]) {
                            liElement.popover({
                                content: data[i][2],
                                trigger: 'hover',
                                placement: 'top'
                            });
                        }
                        invasiveList.push(data[i][0])
                    }
                    Shared.InvasiveList = invasiveList;
                    self.filtersReady['invasions'] = true;

                    if (self.initialSelectedCategory.includes('alien') && self.initialSelectedInvasions.length == 0) {
                        $('#all-invasive-checkbox').trigger("click");
                    }
                }
            });

            $.ajax({
                type: 'GET',
                url: listCollectorAPIUrl,
                dataType: 'json',
                success: function (data) {
                    var selected;
                    for (var i = 0; i < data.length; i++) {
                        if ($.inArray(data[i], self.initialSelectedCollectors) > -1) {
                            selected = 'selected';
                        } else {
                            selected = '';
                        }

                        if (data[i]) {
                            $('#filter-collectors').append(`
                                <option value="${data[i]}" ${selected}>${data[i]}</option>`);
                        }
                    }
                    self.filtersReady['collector'] = true;
                    $('#filter-collectors').chosen({});
                }
            });

            // Decision support tools
            $.ajax({
                type: 'GET',
                url: '/api/list-dst/',
                dataType: 'json',
                success: function (data) {
                    var selected;
                    for (var i = 0; i < data.length; i++) {
                        if ($.inArray(data[i], self.initialSelectedDST) > -1) {
                            selected = 'selected';
                        } else {
                            selected = '';
                        }

                        if (data[i]) {
                            $('#filter-decision-support-tools').append(`
                                <option value="${data[i]}" ${selected}>${data[i]}</option>`);
                        }
                    }
                    self.filtersReady['dst'] = true;
                    $('#filter-decision-support-tools').chosen({});
                }
            });

            $.ajax({
                type: 'GET',
                url: listReferenceAPIUrl,
                dataType: 'json',
                success: function (data) {
                    var selected;
                    for (var i = 0; i < data.length; i++) {
                        if ($.inArray(data[i]['id'] + '', self.initialSelectedStudyReference) > -1) {
                            selected = 'selected';
                        } else {
                            selected = '';
                        }
                        if (data[i]) {
                            $('#filter-study-reference').append(`
                                <option value="${data[i]['id']}" ${selected}>${data[i]['reference']}</option>`);
                        }
                    }
                    self.filtersReady['study-reference'] = true;
                    $('#filter-study-reference').chosen({search_contains: true});
                }
            });

            if (!this.currentSort) {
                this.currentSort = this.defaultSort;
                this.$el.find('#sortby-select').change();
            }

            return this;
        },
        isAllFiltersReady: function () {
            var isReady = true;
            $.each(this.filtersReady, function (key, ready) {
                if (!ready) {
                    isReady = false;
                    return false;
                }
            });
            return isReady;
        },
        checkSearch: function (forceSearch) {
            var searchValue = $('#search').val();
            if (searchValue.length > 0 && searchValue.length < 3) {
                $('#search-error-text').show();
                $('.apply-filter').attr("disabled", "disabled");
                $('.search-arrow').addClass('disabled');
            } else {
                $('#search-error-text').hide();
                $('.apply-filter').removeAttr("disabled");
                $('.search-arrow').removeClass('disabled');
            }
            if (forceSearch === true) {
                if (!this.isAllFiltersReady()) {
                    var that = this;
                    setTimeout(function () {
                        that.checkSearch(forceSearch);
                    }, 500);
                    return false;
                }
                this.search(searchValue);
            }
        },
        getSelectedConservationStatus: function () {
            var selected = [];
            $('.conservation-status-wrapper').find('input:checked').each(function () {
                selected.push($(this).val())
            });
            if (selected) {
                return JSON.stringify(selected)
            }
            return '';
        },
        clearSelectedConservationStatus: function () {
            return this.$el.find("#conservation-status").val("").trigger('chosen:updated');
        },
        highlightPanel: function (identifier, state) {
            let panelTitle = $(identifier).prev().find('.subtitle');
            if (state) {
                panelTitle.addClass('filter-panel-selected');
            } else {
                panelTitle.removeClass('filter-panel-selected');
            }
        },
        search: function (searchValue) {

            this.searchResultCollection.clearPagination();

            $('#filter-validation-error').hide();
            Shared.Dispatcher.trigger('siteDetail:updateCurrentSpeciesSearchResult', []);
            if ($('#search-error-text').is(":visible")) {
                return;
            }
            var self = this;
            this.searchPanel.clearSidePanel();
            this.searchPanel.openSidePanel(false);

            $('#search-results-wrapper').html('');

            // modules
            if(filterParameters['modules']) {
                $('.biodiversity-module-title').addClass('filter-panel-selected');
            } else {
                $('.biodiversity-module-title').removeClass('filter-panel-selected');
            }

            // reference category
            var referenceCategory = self.referenceCategoryView.getSelected();
            if (referenceCategory.length > 0) {
                referenceCategory = JSON.stringify(referenceCategory);
                self.referenceCategoryView.highlight(true);
            } else {
                referenceCategory = '';
                self.referenceCategoryView.highlight(false);
            }
            filterParameters['referenceCategory'] = referenceCategory;

            // source collection
            var sourceCollection = self.sourceCollectionView.getSelected();
            if (sourceCollection.length > 0) {
                sourceCollection = JSON.stringify(sourceCollection);
                self.sourceCollectionView.highlight(true);
            } else {
                sourceCollection = '';
                self.sourceCollectionView.highlight(false);
            }
            filterParameters['sourceCollection'] = sourceCollection;

            // GBIF Dataset filter
            var gbifDatasets = self.gbifDatasetView.getSelected();
            if (gbifDatasets.length > 0) {
                gbifDatasets = JSON.stringify(gbifDatasets);
                self.gbifDatasetView.highlight(true);

                // Auto-select GBIF if datasets are selected
                var sourceCollectionArray = sourceCollection ? JSON.parse(sourceCollection) : [];
                if (sourceCollectionArray.indexOf('gbif') === -1) {
                    sourceCollectionArray.push('gbif');
                    sourceCollection = JSON.stringify(sourceCollectionArray);
                    filterParameters['sourceCollection'] = sourceCollection;
                }
            } else {
                gbifDatasets = '';
                self.gbifDatasetView.highlight(false);
            }
            filterParameters['datasetKeys'] = gbifDatasets;

            // Collector filter
            var collectorValue = $("#filter-collectors").val();
            self.highlightPanel('.filter-collectors-row', collectorValue.length > 0);
            if (collectorValue.length === 0) {
                collectorValue = '';
            } else {
                var encodedCollectorValue = [];
                $.each(collectorValue, function (index, value) {
                    encodedCollectorValue.push(encodeURIComponent(value));
                });
                collectorValue = encodeURIComponent(JSON.stringify(
                    collectorValue)
                );
            }
            filterParameters['collector'] = collectorValue;

            // DST filter
            let dstValue = $("#filter-decision-support-tools").val();
            self.highlightPanel('.filter-decision-support-tools-row', dstValue.length > 0);
            if (dstValue.length === 0) {
                dstValue = '';
            } else {
                dstValue = encodeURIComponent(JSON.stringify(
                    dstValue)
                );
            }
            filterParameters['dst'] = dstValue;

            // reference filter
            var referenceValue = $("#filter-study-reference").val();
            self.highlightPanel('.filter-study-reference-row', referenceValue.length > 0);
            if (referenceValue.length === 0) {
                referenceValue = ''
            } else {
                var encodedReferenceValue = [];
                $.each(referenceValue, function (index, value) {
                    encodedReferenceValue.push(encodeURIComponent(value));
                });
                referenceValue = encodeURIComponent(JSON.stringify(
                    referenceValue)
                );
            }
            filterParameters['reference'] = referenceValue;

            // Category filter
            var categoryValue = [];
            $('input[name=category-value]:checked').each(function () {
                categoryValue.push($(this).val())
            });
            if (categoryValue.length === 0) {
                categoryValue = '';
            } else {
                categoryValue = JSON.stringify(categoryValue);
            }
            filterParameters['category'] = categoryValue;

            // Endemic filter
            var endemicValue = [];
            $('input[name=endemic-value]:checked').each(function () {
                endemicValue.push($(this).val())
            });
            if (endemicValue.length === 0) {
                endemicValue = '';
            } else {
                endemicValue = JSON.stringify(endemicValue);
            }
            filterParameters['endemic'] = endemicValue;
            self.highlightPanel('#origin-filter-wrapper', endemicValue.length > 0 || categoryValue.length > 0);

            // Invasions filter
            let invasionsValue = [];
            $('input[name=invasive-value]:checked').each(function () {
                invasionsValue.push($(this).val())
            });
            if (invasionsValue.length === 0) {
                invasionsValue = '';
            } else {
                invasionsValue = JSON.stringify(invasionsValue);
            }
            if ($('#all-invasive-checkbox').prop('checked')) {
                invasionsValue = '';
            }
            filterParameters['invasions'] = invasionsValue;
            self.highlightPanel('#origin-filter-wrapper', invasionsValue.length > 0 || categoryValue.length > 0);

            // Conservation status filter
            filterParameters['conservationStatus'] = this.getSelectedConservationStatus();
            self.highlightPanel('.conservation-status-row', filterParameters['conservationStatus'] !== '[]');

            // Boundary filter
            var boundaryValue = this.spatialFilterView.selectedPoliticalRegions;
            filterParameters['boundary'] = boundaryValue.length === 0 ? '' : JSON.stringify(boundaryValue);

            // User boundary filter
            var userBoundarySelected = Shared.UserBoundarySelected;
            if (userBoundarySelected.length === 0 && boundaryValue.length === 0) {
                Shared.Dispatcher.trigger('map:boundaryEnabled', false);
                Shared.Dispatcher.trigger('map:closeHighlightPinned');
            } else {
                Shared.Dispatcher.trigger('map:boundaryEnabled', true);
            }
            filterParameters['userBoundary'] = userBoundarySelected.length === 0 ? '' : JSON.stringify(userBoundarySelected);

            if (boundaryValue.length > 0) {
                Shared.Dispatcher.trigger('catchmentArea:show-administrative', boundaryValue);
            }

            // Spatial filter
            var spatialFilters = this.spatialFilterView.selectedSpatialFilters;
            filterParameters['spatialFilter'] = spatialFilters.length === 0 ? '' : JSON.stringify(spatialFilters);
            this.spatialFilterView.showBoundary();

            // Advanced spatial filter
            var advSpatialData = this.advancedSpatialFilterView.getSelected();
            var advHasFilters = advSpatialData.groups.some(function (g) {
                return g.clauses.some(function (c) { return c.values && c.values.length > 0; });
            });
            filterParameters['asf'] = advHasFilters ? JSON.stringify(advSpatialData.groups) : '';
            if (filterParameters['asf']) {
                filterParameters['spatialFilter'] = '';
            }

            this.spatialFilterView.highlight(spatialFilters.length !== 0 || advHasFilters);

            // Validation filter
            let validationFilter = [];
            let validated = '';
            $('[name=filter-validation]:checked').each(function () {
                validationFilter.push($(this).attr('value'));
            });
            if (validationFilter.length > 0) {
                validated = JSON.stringify(validationFilter);
                self.highlightPanel('.validation-filter-row', true);
            } else {
                self.highlightPanel('.validation-filter-row', false);
            }
            filterParameters['validated'] = validated;

            // Ecosystem type filter
            let ecosystemTypeFilter = [];
            let ecosystemType = '';
            $('[name=filter-ecosystem-type]:checked').each(function () {
                ecosystemTypeFilter.push($(this).attr('value'));
            });
            if (ecosystemTypeFilter.length > 0) {
                ecosystemType = ecosystemTypeFilter.join();
                self.highlightPanel('.ecosystem-type-row', true);
            } else {
                self.highlightPanel('.ecosystem-type-row', false);
            }
            filterParameters['ecosystemType'] = ecosystemType;

            // ecological category
            var ecologicalConditions = '';
            if (self.selectedEcologicalConditions.length > 0) {
                ecologicalConditions = JSON.stringify(self.selectedEcologicalConditions);
                self.highlightPanel('.ecological-condition', true);
            } else {
                self.highlightPanel('.ecological-condition', false);
            }
            filterParameters['ecologicalCategory'] = ecologicalConditions;

            // Tags
            let selectedTagIds = self.taxonTagsView.getSelected();
            if (selectedTagIds.length) {
                filterParameters['tags'] = JSON.stringify(
                    selectedTagIds.map(function (v) {
                        return decodeURIComponent(v);
                    })
                );
            }

            // Selected module
            filterParameters['module'] = document.querySelector('input[name="module"]:checked').value;
            self.highlightPanel('.thermal-module-filters-wrapper', filterParameters['module'] !== 'occurrence');

            // Search value
            filterParameters['search'] = searchValue;

            // Sort value
            filterParameters['orderBy'] = self.currentSort;

            // Polygon
            filterParameters['polygon'] = self.lassoPanel.getPolygonCoordinates();

            var yearFrom = $('#year-from').html();
            var yearTo = $('#year-to').html();
            var monthSelected = [];
            if ($('#month-selector').find('input:checkbox:checked').length > 0 ||
                yearFrom != this.startYear || yearTo != this.endYear) {
                $('#month-selector').find('input:checkbox:checked').each(function () {
                    monthSelected.push($(this).val());
                });
                filterParameters['yearFrom'] = yearFrom;
                filterParameters['yearTo'] = yearTo;
                filterParameters['months'] = monthSelected.join(',');
                self.highlightPanel('.temporal-scale-row', true);
            } else {
                self.highlightPanel('.temporal-scale-row', false);
            }
            Shared.Dispatcher.trigger('map:closeHighlight');
            Shared.Dispatcher.trigger(Shared.EVENTS.SEARCH.HIT, filterParameters);
            Shared.Dispatcher.trigger('sidePanel:closeSidePanel');
            const keysToCheck = [
                'search', 'collector', 'validated', 'ecosystemType', 'category',
                'yearFrom', 'yearTo', 'userBoundary', 'referenceCategory', 'reference',
                'endemic', 'modules', 'conservationStatus', 'spatialFilter',
                'ecologicalCategory', 'sourceCollection', 'datasetKeys', 'abioticData', 'polygon',
                'boundary', 'dst', 'thermalModule', 'asf'
            ];
            const hasAnyTruthyValue = keysToCheck.some(key => filterParameters[key]);
            if (!hasAnyTruthyValue) {
                Shared.Dispatcher.trigger('cluster:updateAdministrative', '');
                Shared.Router.clearSearch();
                return false;
            }
            this.searchResultCollection.search(
                this.searchPanel,
                filterParameters,
                self.shouldUpdateUrl
            );

            if (!self.shouldUpdateUrl) {
                self.shouldUpdateUrl = true;
            }
        },
        searchFinished: function () { // Called when the search is complete
            Shared.Dispatcher.trigger('search:finished', true);

            if (filterParameters.hasOwnProperty('siteIdOpen')) {
                // If the filter contains siteIdOpen,
                // then fetch the site detail for that id
                if (filterParameters['siteIdOpen']) {
                    Shared.Dispatcher.trigger(
                        'siteDetail:show',
                        filterParameters['siteIdOpen'], '', true);
                }
            }

            // Enable permalink button
            let permalinkButton = $('#permalink-control');
            permalinkButton.removeClass('sub-control-panel-disabled');
            permalinkButton.attr('data-content', 'Copy sharable link for this map');
        },
        searchClick: function () {
            Shared.Dispatcher.trigger('map:clearAllLayers');
            var searchValue = $('#search').val();
            this.search(searchValue);
        },
        searchEnter: function (e) {
            if (e.which === 13) {
                // if (Shared.CurrentState.FETCH_CLUSTERS) {
                //     return true;
                // }
                var searchValue = $('#search').val();
                this.search(searchValue);
            }
        },
        clearFilterBiodiversityModule: function () {
            this.clearClickedModuleSpecies();
            if (filterParameters.hasOwnProperty('modules')) {
                filterParameters['modules'] = '';
            }
        },
        clearFilterModule: function () {
            this.clearClickedModuleSpecies();
            $('#abiotic-data-filter').prop('checked', false);
            $('#thermal-module-filter').prop('checked', false);
        },
        clearSearch: function () {
            Shared.CurrentState.SEARCH = false;
            Shared.Dispatcher.trigger('search:finished', false);
            $('#abiotic-data-filter').prop('checked', false);
            Shared.Router.initializeParameters();
            this.clearClickedModuleSpecies();
            this.spatialFilterView.clearLayers();
            this.searchInput.val('');
            this.searchResultCollection.clearSearchRequest();
            $('.clear-filter').click();
            Shared.Router.clearSearch();
            $('.map-search-result').hide();
            this.searchPanel.clearSidePanel();
            this.clearClickedOriginButton();

            $('[name=filter-ecosystem-type]').prop('checked', true);

            Shared.Dispatcher.trigger('politicalRegion:clear');

            Shared.Dispatcher.trigger('spatialFilter:clearSelected');

            // If advanced spatial filter mode was active, switch back to normal mode and clear it
            if (this.spatialFilterView.advancedMode) {
                this.advancedSpatialFilterView.clearAll();
                this.spatialFilterView.setAdvancedMode(false);
            }

            Shared.Dispatcher.trigger('siteDetail:updateCurrentSpeciesSearchResult', []);
            Shared.Dispatcher.trigger('cluster:updateAdministrative', '');
            Shared.Dispatcher.trigger('clusterBiological:clearClusters');

            Shared.Dispatcher.trigger('map:resetSitesLayer');
            Shared.Dispatcher.trigger('map:refetchRecords');
            $('.subtitle').removeClass('filter-panel-selected');
            Shared.Dispatcher.trigger('map:zoomToDefault');

            // Clear polygon
            this.lassoPanel.clearLasso();

            // Disable permalink
            let permalinkButton = $('#permalink-control');
            permalinkButton.addClass('sub-control-panel-disabled');
            permalinkButton.attr('data-content', 'No filters applied');
        },
        datePickerToDate: function (element) {
            if ($(element).val()) {
                return new Date($(element).val().replace(/(\d{2})-(\d{2})-(\d{4})/, "$2/$1/$3")).getTime()
            } else {
                return '';
            }
        },
        initDateFilter: function () {
            // render slider
            this.startYear = parseInt(min_year_filter);
            this.endYear = parseInt(max_year_filter);
            if (this.startYear === this.endYear) {
                this.startYear -= 1;
            }
            this.yearSlider = NoUiSlider.create($('#year-slider')[0], {
                start: [this.startYear, this.endYear],
                connect: true,
                range: {
                    'min': this.startYear,
                    'max': this.endYear
                },
                step: 1
            });
            $('#year-from').html(this.startYear);
            $('#year-to').html(this.endYear);
            this.yearSlider.on('slide', function doSomething(values, handle, unencoded, tap, positions) {
                $('#year-from').html(Math.floor(values[0]));
                $('#year-to').html(Math.floor(values[1]));
            });

            // create month selector
            var monthSelectorHtml = '';
            var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
            $.each(months, function (index, value) {
                if (index % 4 == 0) {
                    monthSelectorHtml += '<tr>';
                }
                monthSelectorHtml += '<td><input type="checkbox" value="' + (index + 1) + '">' + value + '</td>';
                if (index % 4 == 3) {
                    monthSelectorHtml += '</tr>';
                }
            });
            $('#month-selector').html(monthSelectorHtml);
        },
        clearFilter: function (e) {
            var target = $(e.target);
            target.closest('.row').find('input:checkbox:checked').prop('checked', false);
            target.closest('.row').find('select').val('').trigger("chosen:updated");
            if (target.closest('.row').find('#year-from').length > 0) {
                this.yearSlider.set([this.startYear, this.endYear]);
                target.closest('.row').find('#year-from').html(this.startYear);
                target.closest('.row').find('#year-to').html(this.endYear);
                filterParameters['yearFrom'] = '';
                filterParameters['yearTo'] = '';
                filterParameters['months'] = ''
            }

            $('.ecological-condition').find('.ecological-condition-item').removeClass('selected');
            this.selectedEcologicalConditions = [];

            if (Shared.CurrentState.SEARCH) {
                this.searchClick();
            }
        },
        show: function () {
            this.searchBox.show();
            this.searchPanel.openSidePanel();
            this.$el.find('#search').focus();
            this.searchBoxOpen = true;
        },
        hide: function () {
            this.searchBox.hide();
            this.searchBoxOpen = false;
            this.searchPanel.hide();
        },
        isOpen: function () {
            return this.searchBoxOpen;
        },
        handleOriginBtnClick: function (e) {
            var target = $(e.target);
            if (target.hasClass('dropdown-toggle')) {
                return;
            }
            var origin = target.data('origin');
            if (target.hasClass('selected')) {
                $('#' + origin).prop('checked', false);
                target.removeClass('selected');
            } else {
                $('#' + origin).prop('checked', true);
                target.addClass('selected');
            }
        },
        handleInvasiveDropdown: function (e) {
            e.stopPropagation();
            let target = $(e.target);
            if (target.hasClass('invasive-checkbox')) {
                if (target.is(':checked')) {
                    target.prop('checked', false);
                } else {
                    target.prop('checked', true);
                }
                target = target.parent();
            }
            let maxTry = 10
            let currentTry = 1

            while (!target.hasClass('invasive-dropdown-item') && currentTry < maxTry) {
                target = target.parent();
            }

            let invasiveValue = target.data('invasive-value');
            let inputCheckbox = target.find('input');

            if (invasiveValue === 'all-invasive') {
                if (inputCheckbox.is(":checked")) {
                    inputCheckbox.prop('checked', false);
                    target.parent().find('.invasive-checkbox').prop('checked', false);
                } else {
                    target.parent().find('.invasive-checkbox').prop('checked', true);
                    inputCheckbox.prop('checked', true);
                }
            } else {
                $('#all-invasive-checkbox').prop('checked', false);
                if (inputCheckbox.is(":checked")) {
                    inputCheckbox.prop('checked', false);
                } else {
                    inputCheckbox.prop('checked', true);
                }
            }

            var atLeastOneIsChecked = $('.non-native-origin-dropdown').find('.invasive-checkbox:checked').length > 0;
            if (atLeastOneIsChecked) {
                $('#alien').prop('checked', true);
                $('#non-native-origin-btn').addClass('selected');
            } else {
                $('#alien').prop('checked', false);
                $('#non-native-origin-btn').removeClass('selected');
            }
        },
        handleEndemicDropdown: function (e) {
            e.stopPropagation();
            var target = $(e.target);
            if (target.hasClass('endemic-checkbox')) {
                if (target.is(':checked')) {
                    target.prop('checked', false);
                } else {
                    target.prop('checked', true);
                }
                target = target.parent();
            }
            var endemicValue = target.data('endemic-value');
            var inputCheckbox = target.find('input');

            if (endemicValue === 'all-endemic') {
                if (inputCheckbox.is(":checked")) {
                    inputCheckbox.prop('checked', false);
                    target.parent().find('.endemic-checkbox').prop('checked', false);
                } else {
                    target.parent().find('.endemic-checkbox').prop('checked', true);
                    inputCheckbox.prop('checked', true);
                }
            } else {
                $('#all-endemic-checkbox').prop('checked', false);
                if (inputCheckbox.is(":checked")) {
                    inputCheckbox.prop('checked', false);
                } else {
                    inputCheckbox.prop('checked', true);
                }
            }

            var atLeastOneIsChecked = $('.native-origin-dropdown').find('.endemic-checkbox:checked').length > 0;
            if (atLeastOneIsChecked) {
                $('#indigenous').prop('checked', true);
                $('#native-origin-btn').addClass('selected');
            } else {
                $('#indigenous').prop('checked', false);
                $('#native-origin-btn').removeClass('selected');
            }
        },
        handleNonNativeClicked: function (e) {
            let button = $(e.target);
            if (button.hasClass('selected')) {
                $('#alien').prop('checked', true);
            } else {
                $('#alien').prop('checked', false);
            }
        },
        clearClickedOriginButton: function () {
            this.$el.find('.origin-btn').removeClass('selected');
        },
        handleClearOriginClicked: function (e) {
            this.clearClickedOriginButton();
        },
        handleClearConservationClicked: function (e) {
            this.clearSelectedConservationStatus();
            if (Shared.CurrentState.SEARCH) {
                this.searchClick();
            }
        },
        filtersUpdated: function (filters, firstCall=true) {
            var self = this;
            var allFilters = {};

            var urlParams = new URLSearchParams(filters);
            for (var filter of urlParams.entries()) {
                if (filter[1]) {
                    allFilters[filter[0]] = filter[1];
                }
            }

            // Category
            if (allFilters.hasOwnProperty('category')) {
                var categories = JSON.parse(allFilters['category']);
                var nonNativeSelected = false;
                $.each(categories, function (index, category) {
                    $('#' + category).prop('checked', true);
                    if (category === 'alien' || category === 'translocated') {
                        nonNativeSelected = true;
                    }
                    self.initialSelectedCategory.push(category);
                });
                if (nonNativeSelected) {
                    $('#non-native-origin-btn').addClass('selected');
                }
            }

            // Search module
            if (allFilters.hasOwnProperty('module')) {
                document.getElementById(allFilters['module'] + '-module').checked = true;
                $('[name="module"]').trigger('change');
            }

            // Search module
            if (allFilters.hasOwnProperty('ecosystemType')) {
                let ecosystemTypes = allFilters['ecosystemType'].split(',');
                $('[name=filter-ecosystem-type]').each(function () {
                    if (ecosystemTypes.includes($(this).attr('value'))) {
                        $(this).prop('checked', true);
                    } else {
                        $(this).prop('checked', false);
                    }
                });
            }

            // Taxon tags
            self.initialSelectedTaxonTags = allFilters.hasOwnProperty('tags') ? JSON.parse(allFilters['tags']) : [];
            self.taxonTagsView.setInitialSelection();

            // Collectors
            self.initialSelectedCollectors = [];
            if (allFilters.hasOwnProperty('collector')) {
                self.initialSelectedCollectors = JSON.parse(allFilters['collector']);
            }

            // DST
            self.initialSelectedDST = [];
            if (allFilters.hasOwnProperty('dst')) {
                self.initialSelectedDST = JSON.parse(allFilters['dst']);
            }

            // Study reference
            self.initialSelectedStudyReference = [];
            if (allFilters.hasOwnProperty('reference')) {
                self.initialSelectedStudyReference = JSON.parse(allFilters['reference']);
            }

            // Endemic
            self.initialSelectedEndemic = [];
            if (allFilters.hasOwnProperty('endemic')) {
                self.initialSelectedEndemic = JSON.parse(allFilters['endemic']);
                if (self.initialSelectedEndemic.length > 0) {
                    $('#native-origin-btn').addClass('selected');
                }
            }

            // Endemic
            self.initialSelectedInvasions = [];
            if (allFilters.hasOwnProperty('invasions')) {
                self.initialSelectedInvasions = JSON.parse(allFilters['invasions']);
                if (self.initialSelectedInvasions.length > 0) {
                    $('#non-native-origin-btn').addClass('selected');
                }
            }


            // Reference category
            self.initialSelectedReferenceCategory = [];
            if (allFilters.hasOwnProperty('referenceCategory')) {
                self.initialSelectedReferenceCategory = JSON.parse(allFilters['referenceCategory']);
            }

            // Source collection
            self.initialSelectedSourceCollection = [];
            if (allFilters.hasOwnProperty('sourceCollection')) {
                self.initialSelectedSourceCollection = JSON.parse(allFilters['sourceCollection']);
                if (!firstCall) {
                    self.sourceCollectionView.updateChecked();
                }
            }

            // GBIF Datasets
            if (allFilters.hasOwnProperty('datasetKeys')) {
                self.initialSelectedGbifDatasets = JSON.parse(allFilters['datasetKeys']);
                if (!firstCall) {
                    self.gbifDatasetView.setInitialSelection();
                }
            }

            // Date
            if (allFilters.hasOwnProperty('yearFrom') && allFilters.hasOwnProperty('yearTo')) {
                $('#year-from').html(Math.floor(allFilters['yearFrom']));
                $('#year-to').html(Math.floor(allFilters['yearTo']));
                this.yearSlider.set([allFilters['yearFrom'], allFilters['yearTo']]);
            }

            // Months
            if (allFilters.hasOwnProperty('months')) {
                var months = allFilters['months'].split(',');
                $('#month-selector').find('input:checkbox').each(function () {
                    if ($.inArray($(this).val(), months) > -1) {
                        $(this).prop('checked', true);
                    }
                });
            }

            // Conservation status
            if (allFilters.hasOwnProperty('conservationStatus')) {
                let conservationStatus = JSON.parse(allFilters['conservationStatus']);
                $.each(conservationStatus, function (index, category) {
                    if (category.startsWith("N__")) {
                        $('#conservation-status-national-' + category.replace('N__', '')).prop('checked', true);
                    } else {
                        $('#conservation-status-global-' + category).prop('checked', true);
                    }
                });
            }

            // Sptaial filter
            if (allFilters.hasOwnProperty('spatialFilter') && firstCall) {
                this.spatialFilterView.selectedSpatialFilters = JSON.parse(allFilters['spatialFilter']);
                this.spatialFilterView.addSelectedSpatialFilterLayerFromJSON(allFilters['spatialFilter']);
            }

            // Advanced spatial filter
            if (allFilters.hasOwnProperty('asf') && allFilters['asf'] && firstCall) {
                var asfGroups = JSON.parse(allFilters['asf']);
                if (asfGroups && asfGroups.length > 0) {
                    this.spatialFilterView.setAdvancedMode(true);
                    this.advancedSpatialFilterView.restoreGroups(asfGroups);
                }
            }

            // Boundary
            if (allFilters.hasOwnProperty('boundary')) {
                this.spatialFilterView.selectedPoliticalRegions = JSON.parse(allFilters['boundary']);
            }

            // Species module
            if (allFilters.hasOwnProperty('modules')) {
                filterParameters['modules'] = allFilters['modules'];
                self.initialSelectedModules = allFilters['modules'].split(',');
            }

            // Abiotic data
            if (allFilters.hasOwnProperty('abioticData')) {
                filterParameters['abioticData'] = allFilters['abioticData'];
                if (filterParameters['abioticData'] === 'True') {
                    $('#abiotic-data-filter').prop('checked', true);
                }
            }

            // Thermal module
            if (allFilters.hasOwnProperty('thermalModule')) {
                filterParameters['thermalModule'] = allFilters['thermalModule'];
                if (filterParameters['thermalModule'] === 'True') {
                    $('#thermal-module-filter').prop('checked', true);
                }
            }

            // Rank
            if (allFilters.hasOwnProperty('rank')) {
                filterParameters['rank'] = allFilters['rank'];
            }

            // Sort by
            if (allFilters.hasOwnProperty('orderBy')) {
                filterParameters['orderBy'] = allFilters['orderBy'];
                self.currentSort = allFilters['orderBy'];
                let sortByElement = self.$el.find('#sortby-select');
                if (self.currentSort !== sortByElement.val()) {
                    sortByElement.val(self.currentSort).change();
                }
            }

            // Polygon
            if (allFilters.hasOwnProperty('polygon')) {
                filterParameters['polygon'] = allFilters['polygon'];
                if(allFilters['polygon'].includes(',')) {
                    self.lassoPanel.drawPolygonFromJSON(allFilters['polygon']);
                } else {
                    self.lassoPanel.loadPolygonById(allFilters['polygon']);
                }
            }

            if (allFilters.hasOwnProperty('ecologicalCategory')) {
                self.selectedEcologicalConditions = JSON.parse(allFilters['ecologicalCategory']);
                $.each(self.selectedEcologicalConditions, function (index, ecologicalCondition) {
                   $(".ecological-condition").find("[data-category='" + ecologicalCondition + "']").addClass('selected');
                });
            }

            if (allFilters.hasOwnProperty('siteIdOpen')) {
                filterParameters['siteIdOpen'] = allFilters['siteIdOpen'];
            }
        },
        showMoreSites: function () {
            this.searchResultCollection.fetchMoreSites();
        },
        showMoreTaxa: function () {
            this.searchResultCollection.fetchMoreTaxa();
        },
        onModuleSpeciesClicked: function (e) {
            let $element = $(e.currentTarget);
            let id = $element.data('id');
            let isSelected = $element.hasClass('selected');
            let modulesParameter = filterParameters['modules'].split(',');
            modulesParameter = modulesParameter.filter(n => n);
            if (modulesParameter.length > 0) {
                for (let i = 0; i < modulesParameter.length; i++) {
                    if (parseInt(modulesParameter[i]) === id) {
                        modulesParameter.splice(i, 1);
                    }
                }
            }
            if (isSelected) {
                $element.removeClass('selected');
            } else {
                $element.addClass('selected');
                modulesParameter.push(id);
            }
            filterParameters['modules'] = modulesParameter.join();
        },
        clearClickedModuleSpecies: function () {
            let $moduleContainer = $('.module-filters');
            $.each($moduleContainer.children(), function (index, element) {
                $(element).removeClass('selected');
            });
        },
        handleEcologicalConditionClicked: function (e) {
            let $target = $(e.target);
            let category = '';
            if ($target.hasClass('ecological-condition-item')) {
                category = $target.data('category');
                if ($target.hasClass('selected')) {
                    $target.removeClass('selected');
                    let index = this.selectedEcologicalConditions.indexOf(category);
                    if (index !== -1) this.selectedEcologicalConditions.splice(index, 1);
                } else {
                    this.selectedEcologicalConditions.push(category);
                    $target.addClass('selected');
                }
            }
        },
        handleSortByChanged: function (e) {
            let element = $(e.target);
            this.currentSort = element.val();
            element.find('option').text(function(i,t){
                return this.selected ? (t.startsWith('Sort by : ') ? t : 'Sort by : ' + t) : t.replace('Sort by : ', '');
            });
            if (Shared.CurrentState.SEARCH) {
                // do search
                this.searchClick();
            }
        },
    })

});
