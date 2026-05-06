(function ($) {
    const $page = $('#checklist-page');

    if (!$page.length) {
        return;
    }

    const csrfToken = $page.data('csrf-token');
    const canPublish = $page.data('can-publish') === true || $page.data('can-publish') === 'true';
    const isSuperuser = $page.data('is-superuser') === true || $page.data('is-superuser') === 'true';
    const publishGroupIds = String($page.data('publish-group-ids') || '')
        .split(',')
        .map(function (value) {
            return parseInt(value, 10);
        })
        .filter(function (value) {
            return !Number.isNaN(value);
        });
    const apiBase = $page.data('api-base');

    let nextUrl = null;
    let prevUrl = null;
    let currentPage = 1;

    const $groupSelect = $('#group-select');
    const $statusSelect = $('#status-select');
    const $loading = $('#version-loading');
    const $table = $('#version-table');
    const $tbody = $('#version-tbody');
    const $empty = $('#version-empty');
    const $paginator = $('#paginator-wrapper');
    const $btnPrev = $('#btn-prev');
    const $btnNext = $('#btn-next');
    const $pageInfo = $('#page-info');
    const $pgPrev = $('#pg-prev');
    const $pgNext = $('#pg-next');

    function canPublishGroup(groupId) {
        if (isSuperuser) {
            return true;
        }
        return publishGroupIds.indexOf(parseInt(groupId, 10)) !== -1;
    }

    function updateTableStateAfterRowRemoval() {
        if ($tbody.children('tr').length === 0) {
            $table.hide();
            $empty.show();
            $paginator.hide();
        }
    }

    function getParam(key) {
        return new URLSearchParams(window.location.search).get(key);
    }

    function pushParams(groupId, status) {
        const url = new URL(window.location.href);
        url.searchParams.set('module', groupId);
        if (status) {
            url.searchParams.set('status', status);
        } else {
            url.searchParams.delete('status');
        }
        window.history.pushState({ module: groupId, status: status }, '', url.toString());
    }

    function statusBadge(status, isPublishing) {
        const display = (status === 'draft' && isPublishing) ? 'publishing' : status;
        return '<span class="badge badge-pill badge-' + display + '">' +
            display.charAt(0).toUpperCase() + display.slice(1) +
            '</span>';
    }

    function changesBadge(v) {
        if (v.status !== 'published') {
            return '<span class="text-muted">—</span>';
        }
        const parts = [];
        if (v.additions_count)  parts.push('<span class="text-success font-weight-bold">+' + v.additions_count + '</span>');
        if (v.deletions_count)  parts.push('<span class="text-danger font-weight-bold">-' + v.deletions_count + '</span>');
        if (v.updates_count)    parts.push('<span class="text-primary">~' + v.updates_count + '</span>');
        return parts.length ? parts.join(' ') : '<span class="text-muted">—</span>';
    }

    function fmtDate(iso) {
        return iso
            ? new Date(iso).toLocaleDateString(undefined, {
                year: 'numeric',
                month: 'short',
                day: 'numeric'
            })
            : '—';
    }

    function renderRows(versions) {
        return versions.map(function (v) {
            return `
                <tr>
                    <td>
                        <a href="#" class="font-weight-bold version-detail-link"
                           data-id="${v.id}">${v.version}</a>
                    </td>
                    <td>${statusBadge(v.status, v.is_publishing)}</td>
                    <td>${v.status === 'published'
                        ? v.taxa_count
                        : '<span class="text-muted" title="Taxa snapshot is only available after publishing">—</span>'}</td>
                    <td class="text-nowrap">${changesBadge(v)}</td>
                    <td>${v.doi
                        ? `<a href="${v.doi}" target="_blank" rel="noopener">${v.doi.length > 35 ? v.doi.substring(0, 35) + '…' : v.doi}</a>`
                        : '—'}</td>
                    <td>${fmtDate(v.published_at)}</td>
                    <td>${v.published_by_name || '—'}</td>
                    <td>${v.created_by_name || '—'}</td>
                    <td>
                        ${v.status === 'published'
                            ? `<button class="btn btn-sm btn-outline-secondary export-coldp-btn"
                                       data-id="${v.id}" title="Download ColDP ZIP">
                                   <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14"
                                        fill="currentColor" viewBox="0 0 16 16">
                                       <path d="M.5 9.9a.5.5 0 0 1 .5.5v2.5a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-2.5a.5.5 0 0 1 1 0v2.5a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2v-2.5a.5.5 0 0 1 .5-.5"/>
                                       <path d="M7.646 11.854a.5.5 0 0 0 .708 0l3-3a.5.5 0 0 0-.708-.708L8.5 10.293V1.5a.5.5 0 0 0-1 0v8.793L5.354 8.146a.5.5 0 1 0-.708.708z"/>
                                   </svg>
                               </button>`
                            : ''}
                        ${v.status === 'draft' && canPublishGroup(v.taxon_group)
                            ? (v.is_publishing
                                ? `<span class="text-muted ml-1">
                                       <span class="spinner-border spinner-border-sm" role="status"></span>
                                       Processing…
                                   </span>`
                                : `<button class="btn btn-sm btn-outline-primary publish-version-btn ml-1"
                                           data-id="${v.id}" data-version="${v.version}"
                                           title="Publish checklist version">
                                       Publish
                                   </button>`)
                            : ''}
                        ${v.status === 'published' && canPublishGroup(v.taxon_group)
                            ? `<button class="btn btn-sm btn-outline-danger remove-version-btn ml-1"
                                       data-id="${v.id}" data-version="${v.version}"
                                       title="Remove published checklist version">
                                   Remove
                               </button>`
                            : ''}
                    </td>
                </tr>`;
        }).join('');
    }

    async function loadVersions(url) {
        $loading.show();
        $table.hide();
        $empty.hide();
        $paginator.hide();

        try {
            const resp = await fetch(url, {
                headers: { 'X-Requested-With': 'XMLHttpRequest' }
            });
            if (!resp.ok) {
                throw new Error(`HTTP ${resp.status}`);
            }
            const data = await resp.json();

            nextUrl = data.next;
            prevUrl = data.previous;

            const results = data.results || [];
            if (results.length) {
                $tbody.html(renderRows(results));
                $table.show();
            } else {
                $empty.show();
            }

            if (data.count > results.length || prevUrl) {
                $pageInfo.text(`Page ${currentPage} of ${Math.ceil(data.count / 20)}`);
                $pgPrev.toggleClass('disabled', !prevUrl);
                $pgNext.toggleClass('disabled', !nextUrl);
                $paginator.show();
            }
        } catch (e) {
            $tbody.html(`<tr><td colspan="9" class="text-danger">Failed to load: ${e.message}</td></tr>`);
            $table.show();
        } finally {
            $loading.hide();
        }
    }

    function buildUrl(groupId, status, page) {
        let url = `${apiBase}?taxon_group=${groupId}&page=${page || 1}`;
        if (status) {
            url += `&status=${status}`;
        }
        return url;
    }

    function reload() {
        currentPage = 1;
        const groupId = $groupSelect.val();
        const status = $statusSelect.length ? $statusSelect.val() : '';
        pushParams(groupId, status);
        loadVersions(buildUrl(groupId, status, 1));
        $('#av-group').val(groupId);
    }

    const initGroup = getParam('module') || $groupSelect.find('option:first').val();
    const initStatus = getParam('status') || '';
    $groupSelect.val(initGroup);
    if ($statusSelect.length) {
        $statusSelect.val(initStatus);
    }
    loadVersions(buildUrl(initGroup, initStatus, 1));

    $groupSelect.on('change', reload);
    if ($statusSelect.length) {
        $statusSelect.on('change', reload);
    }

    $btnPrev.on('click', function (e) {
        e.preventDefault();
        if (prevUrl) {
            currentPage -= 1;
            loadVersions(prevUrl);
        }
    });

    $btnNext.on('click', function (e) {
        e.preventDefault();
        if (nextUrl) {
            currentPage += 1;
            loadVersions(nextUrl);
        }
    });

    window.addEventListener('popstate', function (e) {
        const state = e.state || {};
        if (state.module) {
            $groupSelect.val(state.module);
        }
        if ($statusSelect.length && state.status !== undefined) {
            $statusSelect.val(state.status);
        }
        loadVersions(buildUrl($groupSelect.val(), $statusSelect.length ? $statusSelect.val() : '', 1));
    });

    function showError(msg) {
        $('#add-version-error').text(msg).removeClass('d-none');
        $('#add-version-success').addClass('d-none');
    }

    function showSuccess(msg) {
        $('#add-version-success').text(msg).removeClass('d-none');
        $('#add-version-error').addClass('d-none');
    }

    async function loadPreviousVersions(groupId) {
        const $sel = $('#av-previous');
        $sel.empty().append('<option value="">Loading…</option>').prop('disabled', true);
        try {
            const resp = await fetch(
                `${apiBase}?taxon_group=${groupId}&status=published&page_size=100`,
                { headers: { 'X-Requested-With': 'XMLHttpRequest' } }
            );
            const data = await resp.json();
            $sel.empty().append('<option value="">— None (first release) —</option>');
            (data.results || []).forEach(function (v) {
                const dateStr = v.published_at
                    ? new Date(v.published_at).toLocaleDateString(undefined, {
                        year: 'numeric',
                        month: 'short',
                        day: 'numeric'
                    })
                    : 'unpublished';
                $sel.append($('<option>', {
                    value: v.id,
                    text: `${v.version} (${dateStr})`
                }));
            });
        } catch (e) {
            $sel.empty().append('<option value="">— None (first release) —</option>');
        } finally {
            $sel.prop('disabled', false);
        }
    }

    if (canPublish) {
        $('#add-version-modal').on('show.bs.modal', function () {
            $('#add-version-error, #add-version-success').addClass('d-none').text('');
            $('#av-version, #av-doi, #av-notes').val('');
            const groupId = $groupSelect.val();
            $('#av-group').val(groupId);
            loadPreviousVersions(groupId);
        });

        $('#av-group').on('change', function () {
            loadPreviousVersions($(this).val());
        });

        function buildPayload() {
            const version = $('#av-version').val().trim();
            const licenceId = $('#av-licence').val();
            if (!version) {
                showError('Version string is required.');
                return null;
            }
            if (!licenceId) {
                showError('Licence is required.');
                return null;
            }
            const payload = {
                taxon_group: parseInt($('#av-group').val(), 10),
                version: version,
                license: parseInt(licenceId, 10)
            };
            const doi = $('#av-doi').val().trim();
            const notes = $('#av-notes').val().trim();
            const previous = ($('#av-previous').val() || '').trim();
            if (doi) {
                payload.doi = doi;
            }
            if (notes) {
                payload.notes = notes;
            }
            if (previous) {
                payload.previous_version = previous;
            }
            return payload;
        }

        async function saveVersion(publish) {
            const payload = buildPayload();
            if (!payload) {
                return;
            }

            const $btn = publish ? $('#btn-save-publish') : $('#btn-save-draft');
            const origText = $btn.text();
            $btn.prop('disabled', true).text('Saving…');

            try {
                // 1. Create the draft
                const resp = await fetch(apiBase, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-CSRFToken': csrfToken
                    },
                    body: JSON.stringify(payload)
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(function () { return {}; });
                    const nonField = (err.non_field_errors || []).join(' ');
                    if (nonField.includes('taxon_group') && nonField.includes('version')) {
                        showError('Version "' + payload.version + '" already exists for this module. Please use a different version string.');
                    } else {
                        showError(err.detail || nonField || JSON.stringify(err));
                    }
                    return;
                }
                const data = await resp.json();

                if (!publish) {
                    showSuccess(`Draft "${data.version}" saved.`);
                    setTimeout(function () {
                        $('#add-version-modal').modal('hide');
                        reload();
                    }, 600);
                    return;
                }

                // 2. Kick off async publish — server sets is_publishing=True immediately
                //    and returns 202 before the snapshot work completes.
                const pubResp = await fetch(`${apiBase}${data.id}/publish/`, {
                    method: 'POST',
                    headers: { 'X-CSRFToken': csrfToken }
                });
                if (!pubResp.ok) {
                    const err = await pubResp.json().catch(function () { return {}; });
                    showError('Draft saved but publish failed: ' + (err.detail || JSON.stringify(err)));
                    return;
                }

                // 3. Close modal and reload — list now shows "Publishing" status
                $('#add-version-modal').modal('hide');
                reload();
                showPublishingBanner(data.id, data.version);

                // 4. Poll until is_publishing clears (Celery task finished)
                pollPublishing(data.id);

            } catch (e) {
                showError('Request failed: ' + e.message);
            } finally {
                $btn.prop('disabled', false).text(origText);
            }
        }

        let _publishingBanner = null;

        function showPublishingBanner(versionId, versionStr) {
            hidePublishingBanner();
            _publishingBanner = $(`
                <div id="publishing-banner" class="alert alert-info d-flex align-items-center mb-2" role="alert">
                    <span class="spinner-border spinner-border-sm mr-2" role="status"></span>
                    <span>Publishing <strong>${versionStr}</strong> — building taxa snapshot, this may take a moment…</span>
                </div>
            `);
            $('#version-table').before(_publishingBanner);
        }

        function hidePublishingBanner() {
            if (_publishingBanner) {
                _publishingBanner.remove();
                _publishingBanner = null;
            }
            $('#publishing-banner').remove();
        }

        function pollPublishing(versionId) {
            const interval = setInterval(async function () {
                try {
                    const r = await fetch(`${apiBase}${versionId}/`, {
                        headers: { 'X-Requested-With': 'XMLHttpRequest' }
                    });
                    if (!r.ok) { clearInterval(interval); hidePublishingBanner(); reload(); return; }
                    const v = await r.json();
                    if (!v.is_publishing) {
                        clearInterval(interval);
                        hidePublishingBanner();
                        reload();
                    }
                } catch (e) {
                    clearInterval(interval);
                    hidePublishingBanner();
                    reload();
                }
            }, 3000);
        }

        function showGlobalAlert(type, msg) {
            const $alert = $(`<div class="alert alert-${type} alert-dismissible fade show" role="alert">
                ${msg}
                <button type="button" class="close" data-dismiss="alert"><span>&times;</span></button>
            </div>`);
            $('#version-table').before($alert);
            setTimeout(function () { $alert.alert('close'); }, 6000);
        }

        $('#btn-save-draft').on('click', function () {
            saveVersion(false);
        });

        $('#btn-save-publish').on('click', function () {
            if (!confirm('Publish this version now? This cannot be undone.')) {
                return;
            }
            saveVersion(true);
        });
    }

    $(document).on('click', '.export-coldp-btn', function () {
        const versionId = $(this).data('id');
        showDownloadPopup(
            'ZIP',
            'Checklist ZIP',
            function (downloadRequestId) {
                $.ajax({
                    url: `${apiBase}${versionId}/export/`,
                    type: 'POST',
                    headers: { 'X-CSRFToken': csrfToken },
                    data: {
                        download_request_id: downloadRequestId
                    },
                    success: function () {
                        $('#alertModalBody').html(downloadRequestMessage);
                        $('#alertModal').modal({
                            keyboard: false,
                            backdrop: 'static'
                        });
                    },
                    error: function (jqXHR, textStatus) {
                        let errorMessage = 'Failed to start checklist ZIP export.';
                        if (jqXHR.responseJSON && jqXHR.responseJSON.detail) {
                            errorMessage += ' ' + jqXHR.responseJSON.detail;
                        } else if (textStatus) {
                            errorMessage += ' ' + textStatus;
                        }
                        alert(errorMessage);
                    }
                });
            },
            true,
            null,
            false
        );
    });

    $(document).on('click', '.publish-version-btn', async function () {
        const versionId = $(this).data('id');
        const versionLabel = $(this).data('version');

        if (!confirm(`Publish version ${versionLabel} now? This cannot be undone.`)) {
            return;
        }

        try {
            const response = await fetch(`${apiBase}${versionId}/publish/`, {
                method: 'POST',
                headers: { 'X-CSRFToken': csrfToken }
            });
            const data = await response.json().catch(function () {
                return {};
            });
            if (!response.ok) {
                throw new Error(data.detail || `HTTP ${response.status}`);
            }
            reload();
        } catch (error) {
            alert(`Failed to publish version: ${error.message}`);
        }
    });

    $(document).on('click', '.remove-version-btn', async function () {
        const $button = $(this);
        const versionId = $button.data('id');
        const versionLabel = $button.data('version');

        if (!confirm(`Remove published version ${versionLabel}? This will run in the background and cannot be undone.`)) {
            return;
        }

        try {
            const response = await fetch(`${apiBase}${versionId}/delete/`, {
                method: 'POST',
                headers: { 'X-CSRFToken': csrfToken }
            });
            const data = await response.json().catch(function () {
                return {};
            });
            if (!response.ok) {
                throw new Error(data.detail || `HTTP ${response.status}`);
            }
            $button.closest('tr').remove();
            updateTableStateAfterRowRemoval();
            $('#alertModalBody').html(data.message || 'Checklist removal queued.');
            $('#alertModal').modal({
                keyboard: false,
                backdrop: 'static'
            });
        } catch (error) {
            alert(`Failed to remove version: ${error.message}`);
        }
    });

    $(document).on('click', '.version-detail-link', async function (e) {
        e.preventDefault();
        const id = $(this).data('id');
        const $body = $('#version-detail-body');
        $('#version-detail-modal-label').text('Checklist Version');
        $body.html('<div class="text-center py-4"><span class="spinner-border spinner-border-sm"></span> Loading…</div>');
        $('#version-detail-modal').modal('show');

        try {
            const resp = await fetch(`${apiBase}${id}/`, {
                headers: { 'X-Requested-With': 'XMLHttpRequest' }
            });
            if (!resp.ok) {
                throw new Error(`HTTP ${resp.status}`);
            }
            const v = await resp.json();

            $('#version-detail-modal-label').text(`Version ${v.version}`);
            $body.html(`
                <dl class="row mb-0">
                    <dt class="col-sm-4">Module</dt>
                    <dd class="col-sm-8">${v.taxon_group_name || '—'}</dd>

                    <dt class="col-sm-4">Status</dt>
                    <dd class="col-sm-8">${statusBadge(v.status, v.is_publishing)}</dd>

                    <dt class="col-sm-4">Taxa</dt>
                    <dd class="col-sm-8">${v.status === 'published'
                        ? v.taxa_count
                        : '<span class="text-muted">— (snapshot created on publish)</span>'}</dd>

                    <dt class="col-sm-4">Changes</dt>
                    <dd class="col-sm-8">${changesBadge(v) || '—'}</dd>

                    <dt class="col-sm-4">DOI</dt>
                    <dd class="col-sm-8">${v.doi
                        ? `<a href="${v.doi}" target="_blank" rel="noopener">${v.doi}</a>`
                        : '—'}</dd>

                    <dt class="col-sm-4">Previous version</dt>
                    <dd class="col-sm-8">${v.previous_version || '—'}</dd>

                    <dt class="col-sm-4">Notes</dt>
                    <dd class="col-sm-8">${v.notes || '—'}</dd>

                    <dt class="col-sm-4">Created by</dt>
                    <dd class="col-sm-8">${v.created_by_name || '—'}</dd>

                    <dt class="col-sm-4">Created at</dt>
                    <dd class="col-sm-8">${fmtDate(v.created_at)}</dd>

                    <dt class="col-sm-4">Published by</dt>
                    <dd class="col-sm-8">${v.published_by_name || '—'}</dd>

                    <dt class="col-sm-4">Published at</dt>
                    <dd class="col-sm-8">${fmtDate(v.published_at)}</dd>
                </dl>
            `);
        } catch (e) {
            $body.html(`<p class="text-danger">Failed to load: ${e.message}</p>`);
        }
    });
}(jQuery));
