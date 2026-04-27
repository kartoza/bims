import { taxaSidebar } from './taxa_sidebar.js';
import { taxaTable } from "./taxa_table.js";
import { addNewTaxon} from "./add_new_taxon.js";
import { taxonDetail } from "./taxon_detail.js";

let taxaData = [];
let orderField = {
    'iucn_status_full_name': 'iucn_status__category',
    'endemism_name': 'endemism__name'
}

function escapeHtml(s) {
  return String(s || '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}
function tokenize(str) {
  if (!str) return [];
  return str.split(',').map(s => s.trim()).filter(Boolean);
}
function isColorDark(hex) {
  const c = (hex || '').replace('#','');
  if (c.length !== 6) return false;
  const r = parseInt(c.slice(0,2),16), g = parseInt(c.slice(2,4),16), b = parseInt(c.slice(4,6),16);
  return (0.299*r + 0.587*g + 0.114*b) < 186;
}
function renderSimpleBadges(tokens) {
  if (!tokens.length) return '';
  return tokens.map(t => `<span class="badge badge-info">${escapeHtml(t)}</span>`).join('');
}
function renderTagBadges(tokens) {
  if (!tokens.length) return '';
  return tokens.map(tag => {
    const m = tag.match(/^(.*)\s+\((#[0-9A-Fa-f]{6})\)\s*$/);
    if (m) {
      const tagName = escapeHtml(m[1].trim());
      const color = m[2];
      const textColor = isColorDark(color) ? 'white' : 'black';
      return `<span class="badge" style="background-color:${color};color:${textColor};">${tagName}</span>`;
    }
    return `<span class="badge badge-info">${escapeHtml(tag)}</span>`;
  }).join('');
}
function renderDiffString(data, { colored, stackedOnNarrow = true } = {}) {
  if (!data) return '';
  const parts = String(data).split(/\s*→\s*|\s*->\s*/);
  if (parts.length === 2) {
    const [oldStr, newStr] = parts;
    const oldTokens = (oldStr.trim() === '-' ? [] : tokenize(oldStr));
    const newTokens = (newStr.trim() === '-' ? [] : tokenize(newStr));
    const render = colored ? renderTagBadges : renderSimpleBadges;

    const oldHtml = render(oldTokens) || '<span class="text-muted">-</span>';
    const newHtml = render(newTokens) || '<span class="text-muted">-</span>';

    const changed = (oldStr || '').trim() !== (newStr || '').trim();

    return `
      <span class="cell-diff ${changed ? 'cell-changed' : ''} ${stackedOnNarrow ? 'stack-on-narrow' : ''}">
        <span class="diff-old">${oldHtml}</span>
        <span class="arrow text-muted mx-1">→</span>
        <span class="diff-new">${newHtml}</span>
      </span>
    `;
  }
  const tokens = tokenize(data);
  return (colored ? renderTagBadges : renderSimpleBadges)(tokens);
}
function renderTextDiff(data, { stackedOnNarrow = true, showLabels = false } = {}) {
  if (data == null) return '';
  const parts = String(data).split(/\s*→\s*|\s*->\s*/);
  if (parts.length === 2) {
    const [oldRaw, newRaw] = parts;
    const oldText = (oldRaw || '').trim();
    const newText = (newRaw || '').trim();
    const changed = oldText !== newText;

    const oldLabel = showLabels ? '<span class="badge badge-light mr-1">OLD</span>' : '';
    const newLabel = showLabels ? '<span class="badge badge-success mr-1">NEW</span>' : '';

    return `
      <span class="cell-diff ${changed ? 'cell-changed' : ''} ${stackedOnNarrow ? 'stack-on-narrow' : ''}"
            title="${escapeHtml(oldText)} → ${escapeHtml(newText)}">
        <span class="old text-muted">${oldLabel}${escapeHtml(oldText || '-')}</span>
        <span class="arrow text-muted mx-1">→</span>
        <span class="new font-weight-bold">${newLabel}${escapeHtml(newText || '-')}</span>
      </span>
    `;
  }
  return escapeHtml(String(data));
}

async function pollTaskStatus(taskId, { intervalMs = 1500, maxAttempts = 200 } = {}) {
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
        try {
            const res = await fetch(`/api/celery-status/${taskId}/`, { credentials: 'same-origin' });
            if (!res.ok) throw new Error('Status request failed');
            const data = await res.json();

            if (['SUCCESS', 'FAILURE', 'REVOKED'].includes(data.state)) {
                return data;
            }
        } catch (e) {
            console.warn('Polling error:', e);
        }
        await new Promise(r => setTimeout(r, intervalMs));
    }
    throw new Error('Polling timed out');
}

async function startBatchApprove(taxonGroupId, includeChildren = true) {
    $('#processingModal').modal({ backdrop: 'static', keyboard: false });
    $('#processingModal').modal('show');

    try {
        const res = await fetch('/api/approve-group-proposals/', {
            method: 'POST',
            credentials: 'same-origin',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': csrfToken,
            },
            body: JSON.stringify({
                taxon_group_id: parseInt(taxonGroupId, 10),
                include_children: includeChildren
            })
        });

        if (!res.ok) {
            const err = await res.json().catch(() => ({}));
            throw new Error(err.detail || err.message || 'Request failed');
        }

        const { task_id } = await res.json();

        const result = await pollTaskStatus(task_id);

        $('#processingModal').modal('hide');

        const message = result?.result || 'Batch approval finished.';
        $('#statusModalBody').text(
            typeof message === 'string' ? message : JSON.stringify(message)
        );
        $('#statusModal').modal('show');

        $('#statusModal').one('hidden.bs.modal', function () {
            location.reload();
        });
    } catch (e) {
        $('#statusModalBody').text(`Failed to start/complete batch approve: ${e.message}`);
        $('#statusModal').modal('show');
        setTimeout(() => {
            $('#processingModal').modal('hide');
        }, 500)
    }
}

export const taxaManagement = (() => {
    const fullUrl = new URL(window.location.href);
    const urlParams = fullUrl.searchParams;
    const $loadingOverlay = $('.loading');
    const $saveTaxonBtn = $('#saveTaxon');

    let taxaUrlList = '';
    let $sortable = $('#sortable');

    const loading = {
        hide: () => {
            $loadingOverlay.hide();
        },
        show: () => {
            $loadingOverlay.show();
        }
    }

    let selectedTaxonGroup = '';

    function getTaxonGroupValidatedCount() {
        const allTaxaGroups = taxaSidebar.allTaxaGroups(JSON.parse(JSON.stringify(taxaGroups)));
        for (let taxaGroup of allTaxaGroups) {
            $.ajax({
                url: '/api/taxon-group-validated/' + taxaGroup.id + '/',
                headers: {"X-CSRFToken": csrfToken},
                type: 'GET',
                success: function (response) {
                    $('#taxon_group_accepted_validated_' + taxaGroup.id).text(response.accepted_validated);
                    $('#taxon_group_synonym_validated_' + taxaGroup.id).text(response.synonym_validated);
                    if (response.total_unvalidated > 0) {
                        $('#taxon_group_unvalidated_' + taxaGroup.id).show();
                        $('#taxon_group_accepted_unvalidated_' + taxaGroup.id).text(response.accepted_unvalidated);
                        $('#taxon_group_synonym_unvalidated_' + taxaGroup.id).text(response.synonym_unvalidated);
                    }
                }
            });
        }
    }

    function init() {
        loading.hide();
        if (!urlParams.get('selected')) {
            selectedTaxonGroup = $($('#sortable').children()[0]).data('id');
            if (selectedTaxonGroup) {
                urlParams.set('selected', selectedTaxonGroup);
                history.pushState({}, null, fullUrl.href);
            }
        } else {
            selectedTaxonGroup = urlParams.get('selected');
        }

        taxaSidebar.init(updateTaxonGroup, selectedTaxonGroup)
        taxaTable.init(getTaxaList, selectedTaxonGroup)
        addNewTaxon.init(selectedTaxonGroup)

        getTaxonGroupValidatedCount();

        $saveTaxonBtn.on('click', handleSubmitEditTaxon)
        $('#download-csv').on('click', handleDownloadCsv)
        $('#download-pdf').on('click', (e) => handleDownloadPdf(e, 'genus'))
        $('#download-pdf-by-family').on('click', (e) => handleDownloadPdf(e, 'family'))

        const $approveAllBtn = $('#approve-all-proposals-btn');
        if ($approveAllBtn.length) {
            $approveAllBtn.on('click', function (e) {
                e.preventDefault();
                if (!selectedTaxonGroup) return;

                if (!confirm('Approve all UNVALIDATED proposals in this taxon group (including children)?')) {
                    return;
                }
                startBatchApprove(selectedTaxonGroup, true);
            });
        }

        if (userCanEditTaxonGroup) {
            $sortable.sortable({
                handle: '.tg-drag-handle',
                axis: 'y',
                items: '> li',
                stop: function (event, ui) {
                    let $li = $(event.target).children('li');
                    let ids = [];
                    $.each($li, function (index, element) {
                        ids.push($(element).data('id'));
                    });
                    $sortable.sortable("disable");
                    loading.show();
                    $.ajax({
                        url: '/api/update-taxon-group-order/',
                        headers: {"X-CSRFToken": csrfToken},
                        type: 'POST',
                        data: {
                            'taxonGroups': JSON.stringify(ids)
                        },
                        success: function (response) {
                            loading.hide();
                            $sortable.sortable("enable");
                        }
                    });
                }
            });

            $('.taxon-children-list').each(function () {
                let $childList = $(this);
                $childList.sortable({
                    handle: '.tg-drag-handle',
                    axis: 'y',
                    items: '> li',
                    stop: function (event, ui) {
                        let $li = $(event.target).children('li');
                        let ids = [];
                        $.each($li, function (index, element) {
                            ids.push($(element).data('id'));
                        });
                        $childList.sortable("disable");
                        loading.show();
                        $.ajax({
                            url: '/api/update-taxon-group-order/',
                            headers: {"X-CSRFToken": csrfToken},
                            type: 'POST',
                            data: {
                                'taxonGroups': JSON.stringify(ids)
                            },
                            success: function (response) {
                                loading.hide();
                                $childList.sortable("enable");
                            }
                        });
                    }
                });
            });
        }

    }

    function handleSubmitEditTaxon(event) {
        event.preventDefault();
        $saveTaxonBtn.html('Processing...')
        $saveTaxonBtn.attr('disabled', true)
        const $form = $('#editTaxonForm');
        let formData = $form.serialize();
        $.ajax({
            url: $form.attr('action'),
            type: 'PUT',
            data: formData,
            headers: {"X-CSRFToken": csrfToken},
            success: function(response) {
                console.log('Success:', response);
                location.reload();
            },
            error: function(xhr, status, error) {
                console.error('Error:', error);
                alert('Failed to update taxon: ' + error);
            },
            complete: function() {
                $saveTaxonBtn.html('Save Changes');
                $saveTaxonBtn.removeAttr('disabled');
            }
        });
    }

    function handleDownloadPdf(e, order_by = 'genus') {
        if (typeof isPublicView !== 'undefined' && isPublicView) {
            e.preventDefault();
            $('#loginRequiredModal').modal('show');
            return;
        }
        const $target = $(e.target);
        const targetHtml = $target.html();
        const targetWidth = $target.width();
        let pdfChecklistTitle = 'PDF Checklist';
        if (order_by === 'family') {
            pdfChecklistTitle += ' by Family';
        }
        showDownloadPopup('PDF', pdfChecklistTitle, function (downloadRequestId) {
            $target.prop('disabled', true);
            $target.html(`<div style="width: ${targetWidth}px;"><img src="/static/images/default/grid/loading.gif" width="20"/></div>`);
            let downloadUrl = taxaUrlList.replace('/api/taxa-list/', '/download-taxa-list/')
            if (!downloadUrl.includes('?')) {
                downloadUrl += '?';
            }
            downloadUrl += '&downloadRequestId=' + downloadRequestId
            downloadUrl += '&output=pdf'
            downloadUrl += '&orderBy=' + order_by
            fetch(downloadUrl)
                .then((resp) => {
                    $target.prop('disabled', false);
                    $target.html(targetHtml);
                    alert(downloadRequestMessage);
                })
                .catch(() => alert('Cannot download the file'));
        }, true, null, false)
    }

    function handleDownloadCsv(e) {
        if (typeof isPublicView !== 'undefined' && isPublicView) {
            e.preventDefault();
            $('#loginRequiredModal').modal('show');
            return;
        }
        const $target = $(e.target);
        const targetHtml = $target.html();
        const targetWidth = $target.width();
        showDownloadPopup('CSV', 'Taxa List', function (downloadRequestId) {
            $target.prop('disabled', true);
            $target.html(`<div style="width: ${targetWidth}px;"><img src="/static/images/default/grid/loading.gif" width="20"/></div>`);
            let downloadUrl = taxaUrlList.replace('/api/taxa-list/', '/download-taxa-list/')
            if (!downloadUrl.includes('?')) {
                downloadUrl += '?';
            }
            downloadUrl += '&downloadRequestId=' + downloadRequestId
            downloadUrl += '&output=csv'
            fetch(downloadUrl)
                .then((resp) => {
                    $target.prop('disabled', false);
                    $target.html(targetHtml);
                    alert(downloadRequestMessage);
                })
                .catch(() => alert('Cannot download the file'));
        }, true, null, false)
    }

    const onEditTaxonFormChanged = (elm, event='change') => {
        elm.on(event, function (e) {
            $saveTaxonBtn.attr('disabled', false);
        })
    }

    const getValuesAfterArrow = (obj) => {
        let results = {};

        // Helper function to check each value
        function checkValue(key, value) {
            if (typeof value === 'string') {
                const parts = value.split('→');
                if (parts.length > 1) {
                    results[key] = parts[1].trim();
                } else {
                    results[key] = value
                }
            } else {
                results[key] = value;
            }
        }

        // Loop through each key-value pair in the object
        for (const key in obj) {
            checkValue(key, obj[key]);
        }

        return results;
    }

    function formatLoadingMessage() {
        return `<div style="padding-left:50px;">Loading...</div>`;
    }

    function getColumnMapping(dataTable) {
        let columns = dataTable.settings().init().columns;
        let columnMapping = {};
        columns.forEach((column, index) => {
            columnMapping[column.data] = index;
        });
        return columnMapping;
    }

    function updateSortIcon(columnIndex, sortDirection) {
        let headerCell = $('#taxaTable_wrapper thead th').eq(columnIndex);
        headerCell.removeClass('dt-ordering-asc dt-ordering-desc');
        if (sortDirection === 'asc') {
            headerCell.addClass('dt-ordering-asc');
        } else if (sortDirection === 'desc') {
            headerCell.addClass('dt-ordering-desc');
        }
    }

    function replaceTaxonGroup(url, newTaxonGroup) {
        return url.replace(/(taxonGroup=)\d+/, `$1${newTaxonGroup}`);
    }

    function updateTaxonGroup(taxonGroupId) {
        selectedTaxonGroup = taxonGroupId;
        let table = $('#taxaTable').DataTable();
        table.destroy();
        let newParams = new URLSearchParams(window.location.search);
        newParams.set('selected', taxonGroupId);

        if (newParams) {
            let urlParams = new URLSearchParams(newParams);
            let currentUrl = new URL(window.location);
            for (let param of urlParams.entries()) {
                currentUrl.searchParams.set(param[0], param[1]);
            }
            window.history.pushState({}, '', currentUrl);
        }

        let taxaUrlParams = new URLSearchParams(taxaUrlList)
        if (taxaUrlParams.get('taxonGroup') !== newParams.get('selected')) {
            taxaUrlList = replaceTaxonGroup(taxaUrlList, newParams.get('selected'))
        }
        getTaxaList(taxaUrlList, newParams);
    }

    const isColorDark = (hexColor) => {
        hexColor = hexColor.replace('#', '');
        const r = parseInt(hexColor.substring(0, 2), 16);
        const g = parseInt(hexColor.substring(2, 4), 16);
        const b = parseInt(hexColor.substring(4, 6), 16);
        const luminance = (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255;
        return luminance < 0.5;
    }

    const findTaxaWithoutTaxonGroup = (url) => {
        let urlObj = new URL(url, window.location.origin);
        let params = new URLSearchParams(urlObj.search);

        params.delete('taxonGroup');
        let taxon = params.get('taxon');

        urlObj.pathname = '/api/taxa-group-summary/';
        urlObj.search = params.toString();

        let updatedUrl = urlObj.toString();
        if (taxon) {
            // Only search taxa if there is search string
            $.ajax({
                url: updatedUrl,
                dataType: 'json',
                success: function (data) {
                    if (data.length > 0) {
                        let warningDivs = $('<div>')
                        for (let i=0; i < data.length; i++) {
                            let warningDiv = $('<div>');
                            warningDiv.append(
                                `<div class="alert alert-warning" role="alert" style="cursor: pointer">
                                    ${data[i].total} taxon found in ${data[i].taxongroup__name}. Click here to view the taxon group.
                                </div>`
                            )
                            warningDiv.on('click', function () {
                                $('#sortable').find(`[data-id="${data[i].taxongroup}"]`).click();
                            })
                            warningDivs.append(warningDiv);
                        }
                        $('.footer-warning').html(warningDivs)
                    }
                }
            });
        }
    }

    const getTaxaList = (url) => {
        $('.footer-warning').html('');
        taxaUrlList = url;
        let urlParams = new URLSearchParams(window.location.search);
        let initialPage = urlParams.get('page') ? parseInt(urlParams.get('page')) : 1;
        let initialPageSize = urlParams.get('page_size') ? parseInt(urlParams.get('page_size')) : 25;
        let initialSortOrder = urlParams.get('o') ? urlParams.get('o').replace('-', '') : 'canonical_name';
        let initialSortBy = urlParams.get('o') ? (urlParams.get('o').includes('-') ? '-' : '') : '';
        let initialStart = (initialPage - 1) * 20;


        $('.download-button-container').show();
        let tableInitialized = false;

        // Build columns array conditionally before DataTable initialization
        let tableColumns = [
             {
                class: 'dt-control',
                sortable: false,
                data: null,
                defaultContent: '',
            },
            {
              data: "canonical_name",
              render: function (data, type, row) {
                const prettyName = renderTextDiff(row.canonical_name || row.scientific_name);
                return `${prettyName}<br/>${row.nameHTML ? '' : ''}${row.gbif_key ? ` <a href="https://www.gbif.org/species/${row.gbif_key}" target="_blank"><span class="badge badge-warning">GBIF</span></a>` : ''}${row.iucn_url ? ` <a href="${row.iucn_url}" target="_blank"><span class="badge badge-danger">IUCN</span></a>` : ''}${!row.validated ? ' <span class="badge badge-secondary">Unvalidated</span>' : ''}<input type="hidden" class="proposal-id" value="${row.proposal_id}" />`;
              },
              className: "min-width-150"
            },
            { data: "author", className: "min-width-100", render: (d) => renderTextDiff(d) },
            {
              data: "family",
              className: "min-width-100",
              render: (data, type, row) => {
                const status = String(row.taxonomic_status || '').trim().toUpperCase();
                const isSynonym = status.includes('SYNONYM');
                return isSynonym ? '' : renderTextDiff(data);
              }
            },
            { data: "taxonomic_status", className: "min-width-100", render: (d) => renderTextDiff(d) },
            {
              data: "accepted_taxonomy_name",
              className: "min-width-100",
              render: function(d) {
                if (!d) return '';
                // Extract plain name from possible diff string ("old → new")
                const parts = String(d).split(/\s*→\s*|\s*->\s*/);
                const rawName = (parts.length === 2 ? parts[1] : parts[0]).trim();
                const canonicalName = taxonDetail.getCanonicalName(rawName);
                if (!canonicalName) return renderTextDiff(d);
                const safeName = canonicalName.replace(/"/g, '&quot;');
                return `<a href="#" class="accepted-taxon-link" data-accepted-name="${safeName}">${renderTextDiff(d)}</a>`;
              }
            },
            { data: "rank", className: "min-width-100", render: (d) => renderTextDiff(d) },
            {
              data: "biographic_distributions",
              className: "min-width-100",
              sortable: false,
              render: function (data) {
                return renderDiffString(data, { colored: false });
              }
            },
            {
              data: "tag_list",
              sortable: false,
              className: "min-width-100",
              searchable: false,
              render: function (data) {
                return renderDiffString(data, { colored: true });
              }
            }
        ];

        // Only add Records column for non-FADA sites
        if (typeof isFadaSite === 'undefined' || !isFadaSite) {
            tableColumns.push({
              data: "total_records",
              className: "min-width-80 text-center",
              orderable: true,
              render: function (data, type, row) {
                if (type === 'display') {
                  const canonicalName = row.canonical_name || row.scientific_name || '';
                  const taxonGroupId = row.taxon_group?.id || '';
                  return `<a href="#" class="view-taxon-records"
                    data-taxon-id="${row.id}"
                    data-canonical-name="${escapeHtml(canonicalName)}"
                    data-taxon-group-id="${taxonGroupId}"
                    title="View records for this taxon">
                    ${data || 0} <i class="fa fa-search" aria-hidden="true"></i>
                  </a>`;
                }
                return data || 0;
              }
            });
        }

        // Add Actions column
        tableColumns.push({
            "data": "rowActionHTML",
            "sortable": false,
            "searchable": false
        });

        let table = $('#taxaTable').DataTable({
            "processing": true,
            "serverSide": true,
            "ordering": true,
            "order": [],
            "scrollX": true,
            "autoWidth": false,
            "fixedColumns": {
                "start": 0,
                "end": 1
            },
            scrollY: 'calc(100vh - 325px)',
            "scrollCollapse": true,
            "initComplete": function(settings, json) {
                if (!tableInitialized) {
                    let columnIdx = settings.aoColumns.findIndex(col => col.data === (initialSortOrder ? initialSortOrder : 'canonical_name'));
                    let initialOrder = [columnIdx, initialSortBy.includes('-') ? 'desc' : 'asc'];
                    this.api().order(initialOrder);
                    let columnMapping = getColumnMapping(table);
                    let columnIndex = columnMapping[initialSortOrder];
                    updateSortIcon(columnIndex, initialSortBy.includes('-') ? 'desc' : 'asc');
                    tableInitialized = true;
                }
            },
            "drawCallback": function (settings) {
                $(".dropdown-action").each(function () {
                    let parent, dropdownMenu, left, top;
                    $(this).off('show.bs.dropdown hidden.bs.dropdown'); // Prevent multiple bindings
                    $(this).on('show.bs.dropdown', function () {
                        parent = $(this).parent();
                        dropdownMenu = $(this).find('.dropdown-menu');
                        left = dropdownMenu.offset().left - $(window).scrollLeft();
                        top = dropdownMenu.offset().top - $(window).scrollLeft();
                        $('body').append(dropdownMenu.css({position: 'fixed', left: left, top: top}).detach());
                    });
                    $(this).on('hidden.bs.dropdown', function () {
                        $(this).append(dropdownMenu.css({position: 'absolute', left: false, top: false}).detach());
                    });
                });

                $('.edit-taxon').off('click').on('click', function(event) {
                    event.preventDefault();
                    if (!taxaData) return false;
                    let data = taxaData.find(taxon => taxon.id === $(this).parent().data('id'));
                    if (data) {
                        window.location.href = `/taxonomy/edit/${data.taxon_group.id}/${data.id}/?next=${encodeURIComponent(location.href)}`;
                    }
                });

                $('.edit-taxon-in-admin').off('click').on('click', function(event) {
                    event.preventDefault();
                    if (!taxaData) return false;

                    let data = taxaData.find(taxon => taxon.id === $(this).parent().data('id'));
                    if (data) {
                        let width = 800;
                        let height = 600;
                        let left = (screen.width / 2) - (width / 2);
                        let top = (screen.height / 2) - (height / 2);
                        let adminUrl = `/admin/bims/taxonomy/${data.id}/change/?_popup=1`;
                        let popup = window.open(adminUrl, 'EditTaxonAdmin', `width=${width},height=${height},top=${top},left=${left}`);

                        let popupInterval = setInterval(function() {
                            if (popup.closed) {
                                clearInterval(popupInterval);
                                if (window.wasFormSaved) {
                                    window.location.reload();
                                }
                            }
                        }, 500);
                    }
                });

                $('.remove-taxon-from-group').off('click').on('click', function (event) {
                    let taxaId = $(this).parent().data('id');
                    let taxon = taxaData.find(t => t.id === taxaId);
                    let childrenCount = taxon ? (taxon.children_count || 0) : 0;
                    let otherGroupCount = taxon ? (taxon.other_group_count || 0) : 0;
                    taxaTable.handleRemoveTaxonFromGroup(taxaId, childrenCount, otherGroupCount);
                });

                $('.reject-taxon').off('click').on('click', function (event) {
                    event.preventDefault();
                    taxaTable.handleRejectTaxon($(this).parent().data('id'));
                });

                $('.validate-taxon').off('click').on('click', function (event) {
                    event.preventDefault();
                    taxaTable.handleValidateTaxon($(this).parent().data('id'));
                });

                $('.view-taxon-records').off('click').on('click', function (event) {
                    event.preventDefault();
                    let canonicalName = $(this).data('canonical-name') || '';
                    let taxonGroupId = $(this).data('taxon-group-id') || '';

                    if (canonicalName) {
                        let searchParams = new URLSearchParams({
                            'taxon': '',
                            'search': canonicalName,
                            'siteId': '',
                            'collector': '',
                            'category': '',
                            'yearFrom': '',
                            'yearTo': '',
                            'months': '',
                            'boundary': '',
                            'userBoundary': '',
                            'referenceCategory': '',
                            'spatialFilter': '',
                            'reference': '',
                            'endemic': '',
                            'invasions': '',
                            'conservationStatus': '[]',
                            'modules': taxonGroupId,
                            'validated': '',
                            'sourceCollection': '',
                            'module': 'occurrence',
                            'ecologicalCategory': '',
                            'rank': '',
                            'tags': '',
                            'siteIdOpen': '',
                            'orderBy': 'name',
                            'polygon': '',
                            'dst': '',
                            'ecosystemType': ''
                        });

                        let url = `/map/#search/${encodeURIComponent(canonicalName)}/${searchParams.toString()}`;
                        window.location.href = url;
                    }
                });
            },
            "displayStart": initialStart,
            "ajax": function (data, callback, settings) {
                let page = urlParams.has('page') && settings.iDraw === 1 ? initialPage : Math.ceil(data.start / data.length) + 1;
                let pageSize = data.length;
                let sortBy = urlParams.get('o') && settings.iDraw == 1 ? initialSortOrder : (data.order.length ? data.columns[data.order[0].column].data : 'canonical_name');
                let sortOrder = urlParams.get('o') && settings.iDraw === 1 ? initialSortBy : (data.order.length && data.order[0].dir === 'desc' ? '-' : '');
                let order = sortOrder + sortBy;

                var currentUrl = new URL(window.location);
                currentUrl.searchParams.set('page', page);
                currentUrl.searchParams.set('page_size', pageSize);
                currentUrl.searchParams.set('o', order);
                window.history.pushState({}, '', currentUrl)

                let orderParam = sortOrder + (orderField.hasOwnProperty(sortBy) ? orderField[sortBy] : sortBy);

                $('#approve-all-proposals-btn').hide();

                $.ajax({
                    url: url,
                    data: {o: orderParam, page: page, page_size: pageSize},
                    success: function (response) {
                        loading.hide();
                        taxaData = response.results;
                        $('#add-taxon-btn').hide();

                        if (response['is_expert']) {
                            $('#add-taxon-btn').show();
                            $('#approve-all-proposals-btn').show();
                        }

                        if (response.count === 0) {
                            // Find without taxon group id
                            findTaxaWithoutTaxonGroup(url)
                        }

                        $.each(response.results, function (index, data) {
                            let name = data.canonical_name || data.scientific_name;
                            let taxonomicStatusHTML = (data.taxonomic_status && data.taxonomic_status.toLowerCase() === 'synonym') ?
                                ` <span class="badge badge-info">Synonym</span>` : '';
                            let gbifHTML = data.gbif_key ? ` <a href="https://www.gbif.org/species/${data.gbif_key}" target="_blank"><span class="badge badge-warning">GBIF</span></a>` : '';
                            let iucnHTML = data.iucn_url ? ` <a href="${data.iucn_url}" target="_blank"><span class="badge badge-danger">IUCN</span></a>` : '';
                            let validatedHTML = !data.validated ? '<span class="badge badge-secondary">Unvalidated</span>' : '';

                            data.nameHTML = name + '<br/>' + gbifHTML + iucnHTML + validatedHTML + `<input type="hidden" class="proposal-id" value="${data.proposal_id}" />`;
                            if (data['can_edit']) {
                                let $rowAction = $('.row-action').clone(true, true).removeClass('row-action');
                                if (!data['validated']) {
                                    $rowAction.find('.btn-validated-container').hide();
                                    if (data['can_be_validated']) {
                                        $rowAction.find('.btn-unvalidated-container').show();
                                    } else {
                                        $rowAction.find('.btn-unvalidated-container').hide();
                                    }
                                } else {
                                    $rowAction.find('.btn-validated-container').show();
                                    $rowAction.find('.btn-unvalidated-container').hide();
                                }
                                $rowAction.find('.dropdown-menu').attr('data-id', data['id']);
                                $rowAction.show();
                                data.rowActionHTML = $rowAction.html();
                            } else {
                                data.rowActionHTML = '';
                            }
                        });
                        callback({
                            draw: data.draw,
                            recordsTotal: response.count,
                            recordsFiltered: response.count,
                            data: response.results
                        });
                    }
                });
            },
            "columns": tableColumns,
            "pageLength": initialPageSize,
            "pagingType": "simple_numbers",
            "searching": false
        });

        const detailRows = [];
        const taxonCache = {};
        const acceptedTaxonCache = {};

        async function fetchTaxonRecord(taxonId) {
            const response = await fetch(`/api/taxon/${taxonId}`);
            if (!response.ok) {
                throw new Error(`Failed to fetch taxon ${taxonId}`);
            }
            return response.json();
        }

        async function fetchAcceptedHierarchy(taxonId) {
            if (acceptedTaxonCache[taxonId]) {
                return acceptedTaxonCache[taxonId];
            }
            const accepted = await fetchTaxonRecord(taxonId);
            acceptedTaxonCache[taxonId] = accepted;
            return accepted;
        }

        $(window).resize(function () {
            table.columns.adjust().draw();
        });

        // Event listener for detail rows
        table.on('click', 'tbody td.dt-control', async function (event) {
            let tr = event.target.closest('tr');
            let proposalId = $(this).closest('tr').find('.proposal-id').val();

            let row = table.row(tr);
            let idx = detailRows.indexOf(tr.id);

            if (row.child.isShown()) {
                tr.classList.remove('details');
                row.child.hide();
                detailRows.splice(idx, 1);
            } else {
                tr.classList.add('details');

                if (!taxonCache[tr.id]) {
                    row.child(formatLoadingMessage()).show();
                    try {
                        const taxonId = tr.id.replace('row_', '');
                        let data = await fetchTaxonRecord(taxonId);
                        if (taxonDetail.shouldUseAcceptedHierarchy(data) && data.accepted_taxonomy) {
                            try {
                                data._accepted_detail = await fetchAcceptedHierarchy(data.accepted_taxonomy);
                            } catch (acceptedError) {
                                console.warn('Failed to load accepted taxon for hierarchy', acceptedError);
                            }
                        }
                        taxonCache[tr.id] = data;
                        row.child(taxonDetail.formatDetailTaxon(data)).show();
                    } catch (error) {
                        console.error('Error fetching taxon data:', error);
                        row.child('<div style="padding-left:50px; color: red;">Failed to load data</div>').show();
                        return;
                    }
                } else {
                    const cached = taxonCache[tr.id];
                    if (taxonDetail.shouldUseAcceptedHierarchy(cached) && cached.accepted_taxonomy && !cached._accepted_detail) {
                        try {
                            cached._accepted_detail = await fetchAcceptedHierarchy(cached.accepted_taxonomy);
                        } catch (acceptedError) {
                            console.warn('Failed to load accepted taxon for hierarchy', acceptedError);
                        }
                    }
                    row.child(taxonDetail.formatDetailTaxon(cached)).show();
                }

                if (proposalId && proposalId !== 'null') {
                    try {
                        let response = await fetch(`/api/taxon-proposal/${proposalId}`);
                        let data = await response.json();
                        if (taxonDetail.shouldUseAcceptedHierarchy(data) && data.accepted_taxonomy) {
                            try {
                                data._accepted_detail = await fetchAcceptedHierarchy(data.accepted_taxonomy);
                            } catch (proposalAcceptedError) {
                                console.warn('Failed to load accepted hierarchy for proposal', proposalAcceptedError);
                            }
                        }
                        let $tr = $(row.child())
                        let $changes = $('<div class="taxon_proposal"><div>Updates</div></div>')
                        $changes.append(taxonDetail.formatDetailTaxon(data))
                        $tr.find('td').append($changes);
                    } catch (error) {
                        console.error('Error fetching taxon data:', error);
                        return;
                    }
                }

                if (idx === -1) {
                    detailRows.push(tr.id);
                }
            }
        });

        // Redraw event to open details for stored rows
        table.on('draw', () => {
            detailRows.forEach((id) => {
                let el = document.querySelector('#' + id + ' td.dt-control');
                if (el) {
                    el.dispatchEvent(new Event('click', { bubbles: true }));
                }
            });
        });

        // Navigate to accepted taxon when link in detail row is clicked
        $(document).on('click', '.accepted-taxon-link', function(e) {
            e.preventDefault();
            e.stopPropagation();
            const acceptedName = $(this).data('accepted-name');
            if (acceptedName) {
                const currentUrl = new URL(window.location.href);
                const newParams = new URLSearchParams();
                const selected = currentUrl.searchParams.get('selected') || '';
                if (selected) {
                    newParams.set('selected', selected);
                }
                newParams.set('taxonomic_status', 'ACCEPTED');
                newParams.set('taxon', acceptedName);
                window.location.search = newParams.toString();
            }
        });

        $("#add-taxon-input").on("keydown", function(event) {
            if(event.which === 13) {
                $('#find-taxon-button').click();
            }
        });
    }

    return {
        init,
    }
})();

document.addEventListener('DOMContentLoaded', taxaManagement.init);
