/**
 * "Publish as live layer" dialog.
 *
 * Saves the current filter as an OGC layer (/api/ogc-layers/) and shows
 * the URL to paste in a GIS client. The same dialog lists the layers of the
 * user, where a URL can be replaced (rotate) or the layer removed.
 *
 * Usage: OgcLiveLayer.open({query: '?taxon=1&siteId=2', name: 'Site 2'})
 *
 * Loads as an AMD module on the map page, or as a plain script (window.
 * OgcLiveLayer) on pages without requirejs, such as the spatial dashboard.
 */
(function (factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery', 'underscore', 'bootstrap'], factory);
    } else {
        window.OgcLiveLayer = factory(window.jQuery, window._);
    }
})(function ($, _) {
    const API = '/api/ogc-layers/';
    const MODAL_ID = 'ogc-live-layer-modal';

    // Read when needed: pages that only list layers do not have them
    const template = () => _.template($('#ogc-live-layer-template').html())();
    const instructions = () =>
        $('#ogc-live-layer-instructions-template').html();

    const state = {query: '', previewXhr: null, previewTimer: null};

    function mineRoot() {
        return $modal().find('.ogc-tab[data-tab="mine"]');
    }

    function $modal() {
        return $('#' + MODAL_ID);
    }

    function ajax(method, url, data) {
        return $.ajax({
            url: url,
            type: method,
            data: data ? JSON.stringify(data) : undefined,
            contentType: 'application/json',
            dataType: 'json',
            headers: {'X-CSRFToken': csrfmiddlewaretoken}
        });
    }

    function errorMessage(xhr) {
        const body = xhr.responseJSON;
        if (!body) {
            return xhr.status === 429 ?
                'Too many requests. Try again in a moment.' :
                'Something went wrong. Try again.';
        }
        if (typeof body === 'string') {
            return body;
        }
        if (body.detail) {
            return body.detail;
        }
        return _.flatten(_.values(body)).join(' ');
    }

    function copyText(text) {
        if (navigator.clipboard && navigator.clipboard.writeText) {
            return navigator.clipboard.writeText(text).then(
                () => true, () => false);
        }
        const $tmp = $('<textarea>').val(text).appendTo('body').select();
        let ok = false;
        try {
            ok = document.execCommand('copy');
        } catch (e) {
            ok = false;
        }
        $tmp.remove();
        return $.Deferred().resolve(ok).promise();
    }

    function urlBlock(url, heading) {
        return `<h6>${_.escape(heading)}</h6>
<div class="input-group mb-2">
  <input type="text" class="form-control ogc-url" readonly
         value="${_.escape(url)}">
  <div class="input-group-append">
    <button class="btn btn-outline-primary ogc-copy" type="button">Copy</button>
  </div>
</div>
<small class="text-muted">This URL is shown once. Copy it now: it cannot be
shown again, only replaced.</small>`;
    }

    function bindCopy($scope) {
        $scope.find('.ogc-copy').off('click').on('click', function () {
            const $btn = $(this);
            const $input = $btn.closest('.input-group').find('.ogc-url');
            $input.select();
            copyText($input.val()).then(function (ok) {
                $btn.text(ok ? 'Copied' : 'Press Ctrl+C');
            });
        });
    }

    function showTab(name) {
        const $m = $modal();
        $m.find('.nav-link').removeClass('active')
            .filter('[data-tab="' + name + '"]').addClass('active');
        $m.find('.ogc-tab').hide().filter('[data-tab="' + name + '"]').show();
        $m.find('.ogc-create').toggle(name === 'publish' &&
            !$m.find('.ogc-result').is(':visible'));
        if (name === 'mine') {
            loadMine(mineRoot());
        }
    }

    function setStatus(cls, html) {
        $modal().find('.ogc-status')
            .attr('class', 'ogc-status alert alert-' + cls).html(html);
    }

    function selections() {
        const $m = $modal();
        return {
            include_sensitive: $m.find('#ogc-include-sensitive').is(':checked'),
            include_private: $m.find('#ogc-include-private').is(':checked')
        };
    }

    const STATUS_TEXT = {
        empty_filter: 'No filter is applied. Apply at least one filter, ' +
            'such as a taxon, site, year range or area, before ' +
            'publishing a live layer.',
        unsupported: 'This filter cannot be published as a live layer.',
        not_allowed: 'You are not allowed to include this data.',
        busy: 'The server is busy. Try again in a moment.',
        disabled: 'Live layers are switched off on this server.'
    };

    function preview() {
        const $m = $modal();
        const $create = $m.find('.ogc-create').prop('disabled', true);
        setStatus('info', 'Checking the filter...');
        if (state.previewXhr) {
            state.previewXhr.abort();
        }
        const choices = selections();
        $m.find('.ogc-data-warning').toggle(
            choices.include_sensitive || choices.include_private);
        state.previewXhr = ajax('POST', API + 'preview/', _.extend({
            query_string: state.query
        }, choices)).done(function (data) {
            $m.find('.ogc-sensitive').toggle(!!data.can_include_sensitive);
            $m.find('.ogc-private').toggle(!!data.can_include_private);
            if (data.status === 'ok') {
                if (data.record_count === 0) {
                    setStatus('warning', 'No records match this filter yet. ' +
                        'You can still create the layer: it will fill as ' +
                        'data is added.');
                } else {
                    setStatus('success', '<b>' +
                        Number(data.record_count).toLocaleString() +
                        '</b> records match this filter (limit ' +
                        Number(data.max_records).toLocaleString() + ').');
                }
                $create.prop('disabled', false);
            } else if (data.status === 'over_cap') {
                setStatus('danger', _.escape(data.message) ||
                    ('This filter matches more than ' +
                        Number(data.max_records).toLocaleString() +
                        ' records. Narrow the filter and try again.'));
            } else {
                setStatus(data.status === 'busy' ? 'warning' : 'danger',
                    _.escape(data.message || STATUS_TEXT[data.status] ||
                        'This filter cannot be published.'));
            }
        }).fail(function (xhr, textStatus) {
            if (textStatus === 'abort') {
                return;
            }
            setStatus('danger', _.escape(xhr.status === 403 ?
                'Please log in first.' : errorMessage(xhr)));
        });
    }

    function create() {
        const $m = $modal();
        const $create = $m.find('.ogc-create').prop('disabled', true);
        const name = $.trim($m.find('#ogc-layer-name').val());
        if (!name) {
            setStatus('danger', 'Enter a name for the layer.');
            $m.find('#ogc-layer-name').focus();
            $create.prop('disabled', false);
            return;
        }
        ajax('POST', API, _.extend({
            name: name, query_string: state.query
        }, selections())).done(function (layer) {
            $m.find('.ogc-form').hide();
            $create.hide();
            const $result = $m.find('.ogc-result').show().html(
                '<div class="alert alert-success">Layer <b>' +
                _.escape(layer.name) + '</b> created with ' +
                Number(layer.record_count || 0).toLocaleString() +
                ' records.</div>' +
                urlBlock(layer.url, 'Layer URL') + instructions());
            bindCopy($result);
            copyText(layer.url).then(function (ok) {
                if (ok) {
                    $result.find('.ogc-copy').text('Copied');
                }
            });
        }).fail(function (xhr) {
            setStatus('danger', _.escape(errorMessage(xhr)));
            $create.prop('disabled', false);
        });
    }

    function renderLayer(layer) {
        const status = layer.is_active ?
            '<span class="badge badge-success">Active</span>' :
            '<span class="badge badge-secondary">Revoked</span>';
        return `<tr data-id="${layer.id}">
<td>${_.escape(layer.name)}<br>${status}</td>
<td><code>${_.escape(layer.masked_url)}</code></td>
<td>${layer.record_count === null || layer.record_count === undefined ?
            '-' : Number(layer.record_count).toLocaleString()}</td>
<td class="text-nowrap">
  ${layer.is_active ? '<button class="btn btn-sm btn-outline-primary ogc-rotate">New URL</button>' : ''}
  <button class="btn btn-sm btn-outline-danger ogc-delete">Delete</button>
</td></tr>`;
    }

    // The list lives in a container with .ogc-mine-message, .ogc-mine-new and
    // .ogc-mine-list: the "My live layers" tab of the dialog, or a page.
    function loadMine($root) {
        const $list = $root.find('.ogc-mine-list').html('Loading...');
        ajax('GET', API + '?mine=1').done(function (data) {
            const layers = data.results || data;
            if (!layers.length) {
                $list.html('<p class="text-muted">You have no live layers ' +
                    'yet.</p>');
                return;
            }
            $list.html('<table class="table table-sm"><thead><tr>' +
                '<th>Name</th><th>URL</th><th>Records</th><th></th></tr>' +
                '</thead><tbody>' + _.map(layers, renderLayer).join('') +
                '</tbody></table>');
        }).fail(function (xhr) {
            $list.html('<div class="alert alert-danger">' +
                _.escape(errorMessage(xhr)) + '</div>');
        });
    }

    function rotate($root, $row) {
        if (!window.confirm('Create a new URL? The current URL stops ' +
            'working at once.')) {
            return;
        }
        ajax('POST', API + $row.data('id') + '/rotate-token/')
            .done(function (layer) {
                const $new = $root.find('.ogc-mine-new').show().html(
                    urlBlock(layer.url, 'New URL for ' + layer.name));
                bindCopy($new);
                loadMine($root);
            }).fail(function (xhr) {
                $root.find('.ogc-mine-message').html(
                    '<div class="alert alert-danger">' +
                    _.escape(errorMessage(xhr)) + '</div>');
            });
    }

    function remove($root, $row) {
        if (!window.confirm('Delete this layer? Its URL stops working for ' +
            'good.')) {
            return;
        }
        ajax('DELETE', API + $row.data('id') + '/').always(function (xhr) {
            if (xhr && xhr.status && xhr.status >= 400) {
                $root.find('.ogc-mine-message').html(
                    '<div class="alert alert-danger">The layer could not ' +
                    'be deleted.</div>');
            }
            loadMine($root);
        });
    }

    function bindList($root) {
        $root.on('click', '.ogc-rotate', function () {
            rotate($root, $(this).closest('tr'));
        });
        $root.on('click', '.ogc-delete', function () {
            remove($root, $(this).closest('tr'));
        });
    }

    function mountList($root) {
        bindList($root);
        loadMine($root);
    }

    function ensureModal() {
        if ($modal().length) {
            return;
        }
        $('body').append(template());
        const $m = $modal();
        $m.on('click', '.nav-link', function (e) {
            e.preventDefault();
            showTab($(this).data('tab'));
        });
        $m.on('click', '.ogc-create', create);
        $m.on('change', '#ogc-include-sensitive, #ogc-include-private',
            preview);
        bindList(mineRoot());
    }

    function open(options) {
        options = options || {};
        if (!is_logged_in) {
            $('#alertModalBody').html('Please log in first.');
            $('#alertModal').modal({keyboard: false, backdrop: 'static'});
            return;
        }
        ensureModal();
        const $m = $modal();
        state.query = options.query || '';
        $m.find('.ogc-form').show();
        $m.find('.ogc-result').hide().empty();
        $m.find('.ogc-mine-new').hide().empty();
        $m.find('.ogc-mine-message').empty();
        $m.find('.ogc-create').show();
        $m.find('#ogc-layer-name').val(options.name || '');
        $m.find('#ogc-include-sensitive, #ogc-include-private')
            .prop('checked', false);
        showTab(options.tab || 'publish');
        $m.modal('show');
        if ((options.tab || 'publish') === 'publish') {
            preview();
        }
    }

    return {open: open, mountList: mountList};
});
