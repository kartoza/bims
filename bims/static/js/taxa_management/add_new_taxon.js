export const addNewTaxon = (() => {
    const $newTaxonParentIdInput = $('.new-taxon-parent-id');
    const $newTaxonFamilyInput = $('.new-taxon-family-name');
    const $newTaxonParent = $('#new-taxon-parent');
    const $taxonForm = $('.new-taxon-form');
    const $addNewTaxonBtn = $taxonForm.find('.add-new-taxon-btn');
    const $newTaxonNameInput = $('#new-taxon-name');
    const $subgenusHeader = $('#subgenus-column-header');
    const $subgenusCell = $('#subgenus-column-cell');
    const $subgenusHintCell = $('#subgenus-hint-cell');
    const $newTaxonSubgenus = $('#new-taxon-subgenus');

    let selectedTaxonGroup = '';
    let subgenusSelect2Initialized = false;

    function initSubgenusSelect2() {
        if (subgenusSelect2Initialized) return;
        $newTaxonSubgenus.select2({
            dropdownParent: $('#addNewTaxonModal'),
            ajax: {
                url: '/species-autocomplete/',
                dataType: 'json',
                delay: 250,
                data: function (params) {
                    return {
                        term: params.term,
                        rank: 'subgenus',
                        taxonGroupId: currentSelectedTaxonGroup,
                        parentId: $newTaxonParent.val() || '',
                    };
                },
                processResults: function (data) {
                    return { results: data };
                },
                cache: false
            },
            allowClear: true,
            placeholder: 'Search for a Subgenus',
            minimumInputLength: 2,
            templateResult: function (item) {
                if (item.loading) return item.text;
                return item.species || item.text;
            },
            templateSelection: function (item) {
                return item.species || item.text;
            },
        });
        subgenusSelect2Initialized = true;
    }

    function showSubgenusColumn() {
        initSubgenusSelect2();
        $subgenusHeader.show();
        $subgenusCell.show();
        $subgenusHintCell.show();
    }

    function hideSubgenusColumn() {
        $subgenusHeader.hide();
        $subgenusCell.hide();
        $subgenusHintCell.hide();
        $newTaxonSubgenus.val(null).trigger('change');
    }

    function showNewTaxonForm(taxonName) {
        let capitalizedTaxonName = taxonName.substr(0, 1).toUpperCase() + taxonName.substr(1).toLowerCase();
        $newTaxonNameInput.val(capitalizedTaxonName);
        $newTaxonParentIdInput.val($newTaxonParent.val());
        $taxonForm.show();
        const authorAutoComplete = $('#author-auto-complete');
        authorAutoComplete.empty();
        authorAutoComplete.val(null).trigger('change');

        $newTaxonParent.empty();
        $newTaxonParent.val(null).trigger('change');
    }

    function normalizeStatus(status) {
        const value = (status || '').toString().toUpperCase();
        return value;
    }

    function updateAcceptedLabel(status) {
        const labelEl = document.getElementById('accepted-taxon-column-label');
        const helperEl = document.getElementById('taxon-name-helper');
        if (!labelEl) return;
        const normalized = normalizeStatus(status);
        labelEl.textContent = (!normalized || normalized === 'ACCEPTED') ? 'Parent' : 'Accepted Taxon';
        if (helperEl) {
            helperEl.textContent = (!normalized || normalized === 'ACCEPTED')
                ? 'Provide the binomial name (e.g. Genus species).'
                : 'Provide the full synonym name, including genus.';
        }
    }

    function addNewTaxonToObservedList(name, colId, rank, taxaId = null, parentId = "", authorName = "", taxonomicStatus = "", subgenusId = "") {
        const status = taxonomicStatus || $('#new-taxon-status').val() || '';
        const normalizedStatus = normalizeStatus(status);
        const finalStatus = normalizedStatus || 'ACCEPTED';
        // gbifKey here is the raw GBIF suggest key from the search table (there's
        // no COL equivalent for as-you-type suggestions); the backend resolves it
        // to a Catalogue of Life col_id before the taxon is created/stored.
        let postData = {
            'colId': colId,
            'taxonName': name,
            'rank': rank,
            'taxonGroupId': currentSelectedTaxonGroup,
            'authorName': authorName,
            'taxonomicStatus': finalStatus,
        };
        if (parentId) {
            if (finalStatus === 'ACCEPTED') {
                postData['parentId'] = parentId;
            } else {
                postData['acceptedTaxonomyId'] = parentId;
            }
        }
        if (subgenusId) {
            postData['subgenusId'] = subgenusId;
        }
        let table = $('.find-taxon-table');
        table.hide();
        let loading = $('.find-taxon-loading');
        loading.show();

        if (!taxaId) {
            const submitNewTaxon = function () {
                $.ajax({
                    url: '/api/add-new-taxon/',
                    type: 'POST',
                    headers: {"X-CSRFToken": csrfToken},
                    data: postData,
                    success: function (response) {
                        insertParam('validated', 'False', false, true);
                    },
                    error: function (xhr) {
                        loading.hide();
                        table.show();
                        let msg = 'Failed to add taxon.';
                        let resp = {};
                        try {
                            resp = JSON.parse(xhr.responseText);
                            if (resp.error) msg = resp.error;
                        } catch (e) {}
                        if (xhr.status === 409 && resp.duplicate) {
                            // Same name and parent but a different author may be
                            // a genuine homonym, so let the user confirm.
                            if (confirm(`${msg}\n\nAdd it anyway as a separate taxon?`)) {
                                postData['confirmDuplicate'] = true;
                                loading.show();
                                table.hide();
                                submitNewTaxon();
                                return;
                            }
                        } else {
                            alert(msg);
                        }
                        $taxonForm.show();
                    }
                });
            };
            submitNewTaxon();
            return;
        }
        loading.hide();
        $.ajax({
            url: '/api/add-taxa-to-taxon-group/',
            headers: {"X-CSRFToken": csrfToken},
            type: 'POST',
            data: {
                'taxaIds': JSON.stringify([taxaId]),
                'taxonGroupId': currentSelectedTaxonGroup,
            },
            success: function (response) {
                if (response.rejected && response.rejected.length > 0) {
                    const names = response.rejected.map(t => t.name).join(', ');
                    alert(`The following taxa could not be added because they already belong to another group: ${names}`);
                }
                $('#addNewTaxonModal').modal('toggle');
                insertParam('validated', 'False', false, true);
            },
            error: function (xhr) {
                table.show();
                let msg = 'Failed to add taxon.';
                try {
                    const resp = JSON.parse(xhr.responseText);
                    if (resp.error) msg = resp.error;
                } catch (e) {}
                alert(msg);
            }
        });
    }

    function populateFindTaxonTable(table, data) {
        let tableBody = table.find('tbody');
        tableBody.html('');
        let gbifImage = '<img src="/static/img/GBIF-2015.png" width="50">';
        $.each(data, function (index, value) {
            let source = value['source'];
            let scientificName = value['scientificName'];
            let canonicalName = value['canonicalName'];
            let rank = value['rank'];
            let key = value['key'];
            let taxaId = value['taxaId'];
            let stored = value['storedLocal'];
            let validated = value['validated'];
            let taxonGroupIds = value['taxonGroupIds'];
            let status = value['status'];

            if (source === 'gbif') {
                source = `<a href="https://www.gbif.org/taxon/${key}" target="_blank">${gbifImage}</a>`;
                scientificName = `<a href="https://www.gbif.org/taxon/${key}" target="_blank">${scientificName}</a>`;
            } else if (source === 'local') {
                source = fontAwesomeIcon('database');
            }
            if (stored) {
                stored = fontAwesomeIcon('check', 'green');
                if (!validated) {
                    stored = '<span class="badge badge-secondary">Unvalidated</span>';
                }
            } else {
                stored = fontAwesomeIcon('times', 'red');
            }
            let action = '';
            const normalizedStatus = status || '';
            if (!taxonGroupIds.includes(parseInt(currentSelectedTaxonGroup))) {
                if (typeof restrictTaxonToSingleGroup !== 'undefined' && restrictTaxonToSingleGroup && taxonGroupIds.length > 0) {
                    action = (`<button
                    type="button"
                    class="btn btn-secondary"
                    disabled
                    title="This taxon already belongs to another group">${fontAwesomeIcon('ban')}&nbsp;Already in another group
                   </button>`);
                } else {
                    action = (`<button
                    type="button"
                    class="btn btn-success add-taxon-btn"
                    data-canonical-name="${canonicalName}"
                    data-key="${key}"
                    data-rank="${rank}"
                    data-status="${normalizedStatus}"
                    data-taxa-id="${taxaId}">${fontAwesomeIcon('plus')}&nbsp;ADD
                   </button>`);
                }
            }
            tableBody.append(`<tr>
                    <td>${scientificName}</td>
                    <td>${canonicalName}</td>
                    <td>${rank}</td>
                    <td>${source}</td>
                    <td>${status}</td>
                    <td>${stored}</td>
                    <td>${action}</td>
                </tr>`);
        });
        table.show();
    }

    function handleFindTaxonButton(event) {
        let taxonName = $('#add-taxon-input').val();
        if (!taxonName) {
            return false;
        }
        let table = $('.find-taxon-table');
        table.hide();
        let loading = $('.find-taxon-loading');
        loading.show();
        $taxonForm.hide();

        $.ajax({
            url: `/api/find-taxon/?q=${encodeURIComponent(taxonName)}&taxonGroupId=${currentSelectedTaxonGroup}`,
            type: 'get',
            dataType: 'json',
            success: function (data) {
                if (data.length > 0) {
                    populateFindTaxonTable(table, data);
                } else {
                    showNewTaxonForm(taxonName);
                }
                loading.hide();
            }
        });
    }

    function handleAddNewTaxon(event) {
        let $rank = $taxonForm.find('.new-taxon-rank');
        let $author = $taxonForm.find('#author-auto-complete');
        const status = $('#new-taxon-status').val();
        const parentId = $newTaxonParent.val();
        if (!parentId) {
            if (status.toLowerCase() === 'accepted') {
                alert("Missing parent");
            } else {
                alert("Missing accepted taxon");
            }
            return;
        }
        const subgenusId = $subgenusCell.is(':visible') ? ($newTaxonSubgenus.val() || '') : '';
        addNewTaxonToObservedList($newTaxonNameInput.val(), '', $rank.val(), null, parentId, $author.val(), status, subgenusId);
        $taxonForm.hide();
        $newTaxonFamilyInput.val("");
        $newTaxonParentIdInput.val("");
    }

    function clearForm() {
        $('#add-taxon-input').val('');

        const table = $('.find-taxon-table');
        table.hide();
        table.find('tbody').empty();

        $('.find-taxon-loading').hide();

        $taxonForm.show();
        $newTaxonNameInput.val('');
        $newTaxonFamilyInput.val('');
        $newTaxonParentIdInput.val('');

        $newTaxonParent.empty();
        $newTaxonParent.val(null).trigger('change');

        hideSubgenusColumn();

        const authorAutoComplete = $('.author-auto-complete');
        authorAutoComplete.val(null).trigger('change');
    }

    function init(_selectedTaxonGroup) {

        $('#addNewTaxonModal').on('shown.bs.modal', function (e) {
            const authorAutoComplete = $('#author-auto-complete');
            authorAutoComplete.empty();
            authorAutoComplete.val(null).trigger('change');

            $newTaxonParent.empty();
            $newTaxonParent.val(null).trigger('change');
            updateAcceptedLabel($('#new-taxon-status').val());
        });

        $('#addNewTaxonModal').on('hidden.bs.modal', function () {
            clearForm();
        });

        selectedTaxonGroup = _selectedTaxonGroup;

        $('#find-taxon-button').on('click', handleFindTaxonButton);
        $addNewTaxonBtn.on('click', handleAddNewTaxon);

        $newTaxonParent.on('change', function () {
            const data = $newTaxonParent.select2('data');
            if (data && data.length > 0 && (data[0].rank || '').toUpperCase() === 'GENUS') {
                showSubgenusColumn();
            } else {
                hideSubgenusColumn();
            }
        });

        $(document).on('click', '.add-taxon-btn', function() {
            const button = $(this);
            const name = button.data('canonical-name');
            const colId = button.data('key');
            const rank = button.data('rank');
            const taxaId = button.data('taxa-id');
            const status = button.data('status') || '';
            addNewTaxonToObservedList(name, colId, rank, taxaId, "", "", status);
        });

        $('#new-taxon-status').on('change', function () {
            updateAcceptedLabel(this.value);
        });

        updateAcceptedLabel($('#new-taxon-status').val());

        const authorAutoComplete = $('.author-auto-complete');
        authorAutoComplete.select2({
            width: '100%',
            ajax: {
                url: '/author-autocomplete/',
                dataType: 'json',
                delay: 250,
                data: function (params) {
                    return {
                        term: params.term,
                    };
                },
                processResults: function (data) {
                    return {
                        results: data.map(item => {
                            return {
                                id: item.author,
                                text: item.author
                            };
                        }),
                    };
                },
                cache: true
            },
            allowClear: true,
            placeholder: 'Search for an Author',
            minimumInputLength: 3,
            templateResult: (item) => item.text,
            templateSelection: (item) => item.text,
            tags: true,
            createTag: function (params) {
                if (!this.$element.data('add-new-tag')) {
                    return null;
                }
                let term = $.trim(params.term);

                if (term === '') {
                    return null;
                }

                let exists = false;
                this.$element.find('option').each(function(){
                    if ($.trim($(this).text()).toUpperCase() === term.toUpperCase()) {
                        exists = true;
                        return false;
                    }
                });

                if (exists) {
                    return null;
                }

                return {
                    id: term,
                    text: term,
                    newTag: true
                };
            }

        });
    }

    return {
        init,
        clear: clearForm,
    }
})()
