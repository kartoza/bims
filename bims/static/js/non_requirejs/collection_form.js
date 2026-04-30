let oldLat = location_site_lat;
let oldLon = location_site_long;

let taxonAbundanceOnChange = function (e) {
    let value = parseFloat(e.target.value);
    if (value && value > 0) {
        $(e.target).closest('tr').find('.observed').prop('checked', true);
    }
};

let taxaIdList = [];
let collectionIdList = [];

let taxonAutocompleteHandler = {
    source: function (request, response) {
        $.ajax({
            url: '/species-autocomplete/?term=' + encodeURIComponent(request.term) + '&exclude=' + taxaIdList.join() + '&taxonGroupId=' + taxonGroupId,
            type: 'get',
            dataType: 'json',
            success: function (data) {
                response($.map(data, function (item) {
                    return {
                        label: item.species,
                        value: item.id
                    }
                }));
            }
        });
    },
    minLength: 3,
    open: function (event, ui) {
        setTimeout(function () {
            $('.ui-autocomplete').css('z-index', 99);
        }, 0);
    },
    select: function (e, u) {
        e.preventDefault();
        let $target = $(e.target);
        let $parent = $target.parent().parent();
        taxaIdList.push(u.item.value);
        $parent.html(
            '<td scope="row" class="taxon-name">' +
            u.item.label +
            '<input type="hidden" class="taxon-id" value="' + u.item.value + '">' +
            '</td>');
        $parent.append(
            '<td>' +
            '<div class="form-check">' +
            '<input class="form-check-input observed" type="checkbox"' +
            ' value="True"' +
            ' name="' + u.item.value + '-observed">' +
            ' <label class="form-check-label">' +
            ' </label>' +
            '</div>' +
            '</td>');

        let taxonAbundanceInput = $('<input type="number" min="0.001"' +
            ' name="' + u.item.value + '-abundance"' +
            ' class="form-control taxon-abundance"' +
            ' placeholder="0">');
        let tdTaxonAbundance = $('<td>');
        tdTaxonAbundance.append(taxonAbundanceInput);
        $parent.append(tdTaxonAbundance);
        taxonAbundanceInput.change(taxonAbundanceOnChange);
    }
};

$('#latitude').keyup((e) => {
    let $target = $(e.target);
    if (!document.getElementById('update-coordinate').disabled) {
        return;
    }
    if ($target.val() !== oldLat) {
        document.getElementById('update-coordinate').disabled = false;
    }
});
$('#longitude').keyup((e) => {
    let $target = $(e.target);
    if (!document.getElementById('update-coordinate').disabled) {
        return;
    }
    if ($target.val() !== oldLon) {
        document.getElementById('update-coordinate').disabled = false;
    }
});

let markerSource = new ol.source.Vector();
let map = null;
let updateCoordinateHandler = (e) => {
    let latitude = $('#latitude').val();
    let longitude = $('#longitude').val();
    let tableBody = $('#closest-site-table-body');

    if (oldLat !== latitude || oldLon !== longitude) {
    } else {
        return false;
    }

    oldLat = latitude;
    oldLon = longitude;
    document.getElementById('update-coordinate').disabled = true;
    moveMarkerOnTheMap(latitude, longitude);
    if ($('#add-new-site-container').is(":visible")) {
        return false;
    }
    $('#closest-sites-container').show();
    tableBody.html('');
    let radius = 10;
    $.ajax({
        url: '/api/get-site-by-coord/?lon=' + longitude + '&lat=' + latitude + '&radius=' + radius,
        success: function (all_data) {
            $.each(all_data, function (index, data) {
                let row = $('<tr>' +
                    '<td>' + (data['site_code'] ? data['site_code'] : '-') + '</td>' +
                    '<td>' + data['name'] + '</td>' +
                    '<td>' + (data['distance_m'] / 1000).toFixed(2) + ' km </td>' +
                    '<td> <button type="button" data-id="' + data['id'] + '" data-lat="' + data['latitude'] + '" data-lon="' + data['longitude'] + '" class="btn btn-info choose-site">' +
                    '<i class="fa fa-search" aria-hidden="true"></i></button> </td>' +
                    '</tr>');
                row.find('.choose-site').click(chooseSiteHandler);
                tableBody.append(row);
            });
        }
    });
};

$('.close-nearest-list-table').click(() => {
    $('#closest-site-table-body').html('');
    $('#closest-sites-container').hide();
});

let chooseSiteHandler = (e) => {
    e.preventDefault();
    let target = $(e.target);
    if (!target.hasClass('choose-site')) {
        target = target.parent();
    }
    let latitude = parseFloat(target.data('lat'));
    let longitude = parseFloat(target.data('lon'));
    oldLat = latitude.toString();
    oldLon = longitude.toString();
    document.getElementById('update-coordinate').disabled = true;
    let siteId = target.data('id');
    let siteCode = target.parent().parent().children().eq(0).html();
    let siteIdentifier = target.parent().parent().children().eq(1).html();
    if (siteCode !== '-') {
        siteIdentifier = siteCode
    }
    $('#site-id').val(siteId);
    $('#site-identifier').html(siteIdentifier);

    $('#latitude').val(latitude);
    $('#longitude').val(longitude);

    moveMarkerOnTheMap(latitude, longitude);
};

let moveMarkerOnTheMap = (lat, long) => {
    let locationSiteCoordinate = ol.proj.transform([
            parseFloat(long),
            parseFloat(lat)],
        'EPSG:4326',
        'EPSG:3857');
    let iconFeature = new ol.Feature({
        geometry: new ol.geom.Point(locationSiteCoordinate),
    });
    markerSource.clear();
    markerSource.addFeature(iconFeature);
    map.getView().setCenter(locationSiteCoordinate);
};

let oldHeader = $('.header').html();
$('.add-new-site').click((e) => {
    $('#closest-sites-container').hide();
    $('#add-new-site-container').show();
    $('.header').html('Add fish data for new site');
});

$('.close-add-new-site').click((e) => {
    $('#add-new-site-container').hide();
    $('.header').html(oldHeader);
    $('#latitude').val(location_site_lat);
    $('#longitude').val(location_site_long);
    oldLat = location_site_lat;
    oldLon = location_site_long;
    $('#location-site-name').val('');
    $('#location-site-code').val('');
    $('#location-site-description').val('');
    moveMarkerOnTheMap(location_site_lat, location_site_long);
});

function autocomplete (id) {
    const input = $(`#${id}`);
    const input_id = $(`#${id}_id`);
    input.autocomplete({
        source: function (request, response) {
            $.ajax({
                url: '/user-autocomplete/?term=' + encodeURIComponent(request.term),
                type: 'get',
                dataType: 'json',
                success: function (data) {
                    response($.map(data, function (item) {
                        return {
                            label: item.first_name + ' ' + item.last_name,
                            value: item.id
                        }
                    }));
                }
            });
        },
        minLength: 3,
        open: function (event, ui) {
            setTimeout(function () {
                $('.ui-autocomplete').css('z-index', 99);
            }, 0);
        },
        change: function (event, ui) {
            if (ui.item) {
                input_id.val(ui.item.value);
            } else {
                input_id.val(' ');
                input.val('');
            }
        },
        select: function (e, u) {
            e.preventDefault();
            input.val(u.item.label);
            input_id.val(u.item.value);
        }
    });
}

$(function () {
    // Get id list
    $.each($('.taxon-table-body').children(), function (index, tr) {
        let val = $(tr).children().eq(0).find('.taxon-id').val();
        if (typeof val !== 'undefined') {
            taxaIdList.push(val);
        }
        let collectionId = $(tr).children().eq(0).find('.collection-id').val();
        if (typeof collectionId !== 'undefined') {
            collectionIdList.push(collectionId);
        }
    });

    let locationSiteCoordinate = ol.proj.transform([
            parseFloat(location_site_long),
            parseFloat(location_site_lat)],
        'EPSG:4326',
        'EPSG:3857');
    let markerStyle = new ol.style.Style({
        image: new ol.style.Icon(({
            anchor: [0.5, 46],
            anchorXUnits: 'fraction',
            anchorYUnits: 'pixels',
            opacity: 0.75,
            src: '/static/img/map-marker.png'
        }))
    });
    let iconFeature = new ol.Feature({
        geometry: new ol.geom.Point(locationSiteCoordinate),
    });
    markerSource.addFeature(iconFeature);
    const baseLayer = []

    if(bingKey){
        baseLayer.push(
            new ol.layer.Tile({
                source: new ol.source.BingMaps({
                key: bingKey,
                imagerySet: 'AerialWithLabels'
            })
            })
        )
    }
    else {
        baseLayer.push(
            new ol.layer.Tile({
                source: new ol.source.OSM()
            })
        )
    }
    baseLayer.push(
        new ol.layer.Vector({
            source: markerSource,
            style: markerStyle,
        }),
    )

    map = new ol.Map({
        target: 'fish-map',
        layers: baseLayer,
        view: new ol.View({
            center: locationSiteCoordinate,
            zoom: 10
        })
    });

    let options = {
        url: 'https://maps.kartoza.com/geoserver/wms',
        params: {
            layers: 'kartoza:sa_rivers',
            format: 'image/png'
        }
    };

    let riverLayer = new ol.layer.Tile({
        source: new ol.source.TileWMS(options)
    });

    map.addLayer(riverLayer);

    let biodiversityLayersOptions = {
        url: geoserverPublicUrl + 'wms',
        params: {
            LAYERS: locationSiteGeoserverLayer,
            FORMAT: 'image/png8',
            viewparams: 'where:' + defaultWMSSiteParameters
        },
        ratio: 1,
        serverType: 'geoserver'
    };
    let biodiversitySource = new ol.source.ImageWMS(biodiversityLayersOptions);
    let biodiversityTileLayer = new ol.layer.Image({
        source: biodiversitySource
    });
    map.addLayer(biodiversityTileLayer);

    $("#date").datepicker({
        changeMonth: true,
        changeYear: true,
        dateFormat: 'dd/mm/yy',
        onSelect: function (dateText) {
            let $target = $(this);
            if ($target.val()) {
                $target.removeClass('error');
                $target.next().hide();
            }
        }
    });

    $("#embargo-end-date").datepicker({
        changeMonth: true,
        changeYear: true,
        minDate: 0,
        dateFormat: 'dd/mm/yy',
        onSelect: function (dateText) {
            let $target = $(this);
        }
    });

    let $taxonAbundance = $('.taxon-abundance');

    $taxonAbundance.keyup(function () {
        this.value = this.value.replace(/[^0-9\.]/g, '');
    });

    $taxonAbundance.change(taxonAbundanceOnChange);

    autocomplete('owner');
    autocomplete('collector');

    $('#submitBtn').click((e) => {
    });

    let form = $('#fish-form');
    $('#submit').click((event) => {
        event.preventDefault();
        $('#submit').attr('disabled','disabled').addClass('disabled');
        let required_inputs = $('input,textarea,select').filter('[required]:visible');
        let isError = false;
        let alertMessage = '';
        $.each(required_inputs, (index, input) => {
            let $input = $(input);
            if (!$input.val()) {
                isError = true;
                $input.addClass('error');
                $input.keyup((e) => {
                    let $target = $(e.target);
                    if ($target.val()) {
                        $target.removeClass('error');
                        $target.next().hide();
                        $target.unbind();
                    }
                });
                $input.next().show();
            } else {
                $input.unbind();
            }
        });
        if (!$('#selected-source-reference').val()) {
            isError = true;
            alertMessage = 'You must select a source reference <br/>';
        }
        if ($('.observed:checkbox:checked').length === 0) {
            if (!chemicalCollectionRecordCount) {
                isError = true;
                alertMessage += 'You must at least add one collection data';
            }
        } else {
            let alertDiv = $('.alert-danger');
            alertDiv.html('');
            alertDiv.hide();

            // Every observed taxon must have an abundance value greater than zero
            let zeroAbundanceFound = false;
            $('.observed:checkbox:checked').each(function () {
                let $abundanceInput = $(this).closest('tr').find('.taxon-abundance');
                let abundanceVal = parseFloat($abundanceInput.val());
                if (!abundanceVal || abundanceVal <= 0) {
                    $abundanceInput.addClass('error');
                    zeroAbundanceFound = true;
                }
            });
            if (zeroAbundanceFound) {
                isError = true;
                alertMessage += 'All observed taxa must have an abundance value greater than zero.<br/>';
            }
        }
        if (alertMessage) {
            let alertDiv = $('.alert-danger');
            alertDiv.html(alertMessage);
            alertDiv.show();
        }
        if (isError) {
            event.preventDefault();
            event.stopPropagation();
            $('#submit').attr('disabled','').removeClass('disabled');
            $('#confirm-submit').modal('hide');
            setTimeout(function () {
                window.scrollTo(0, 0);
            }, 500);
            return;
        }
        // Get taxa list id
        let $taxaIdList = $('#taxa-id-list');
        $taxaIdList.val(taxaIdList.join());

        // Get taxa list id
        let $collectionIdList = $('#collection-id-list');
        $collectionIdList.val(collectionIdList.join());

        // Fix date
        let originalDate = $('#date').val();
        if (originalDate) {
            let parts = originalDate.split('/');
            let newDate = parts[1] + '/' + parts[0] + '/' + parts[2];
            $('#date').val(newDate);
        }
        let embargoDate = $('#embargo-end-date').val();
        if (embargoDate) {
            let embargoParts = embargoDate.split('/');
            let newEmbargoDate = embargoParts[0] + '/' + embargoParts[1] + '/' + embargoParts[2];
            $('#embargo-end-date').val(newEmbargoDate);
        }

        form.submit();
    });

    $('#add-taxon').click((e) => {
        let $findTaxonContainer = $('.find-taxon-container');
        let $findTaxonContainerClone = $findTaxonContainer.clone();
        $findTaxonContainerClone.removeClass('find-taxon-container');
        $findTaxonContainerClone.find('.taxon-input-name').autocomplete(taxonAutocompleteHandler);
        $findTaxonContainer.after($findTaxonContainerClone.show());
    });

    $('.taxon-input-name').autocomplete(taxonAutocompleteHandler);
    $('#update-coordinate').click(updateCoordinateHandler);

});

$('#add-taxon-input').keyup(function (event) {
    // Number 13 is the "Enter" key on the keyboard
    if (event.keyCode === 13) {
        // Cancel the default action, if needed
        event.preventDefault();
        // Trigger the button element with a click
        $('#find-taxon-button').click();
    }
});

$('#find-taxon-button').click(function () {
    let taxonName = $('#add-taxon-input').val();
    if (!taxonName) {
        return false;
    }
    // Show loading div
    let table = $('.find-taxon-table');
    table.hide();
    let loading = $('.find-taxon-loading');
    loading.show();
    let $newTaxonForm = $('.new-taxon-form');
    $newTaxonForm.hide();

    // Show response list
    $.ajax({
        url: `/api/find-taxon/?q=${encodeURIComponent(taxonName)}&taxonGroupId=${taxonGroupId}`,
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
});

function showNewTaxonForm(taxonName) {
    let $taxonForm = $('.new-taxon-form');
    let $button = $taxonForm.find('.add-new-taxon-btn');
    let $rank = $taxonForm.find('.new-taxon-rank');
    let capitalizedTaxonName = taxonName.substr(0, 1).toUpperCase() + taxonName.substr(1).toLowerCase();
    let $newTaxonNameInput = $('#new-taxon-name');
    $button.click(function () {
        $taxonForm.hide();
        addNewTaxonToObservedList(capitalizedTaxonName, '', $rank.val());
    });
    $newTaxonNameInput.val(capitalizedTaxonName);
    $taxonForm.show();
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
        let status = value['status'];

        if (source === 'gbif') {
            source = `<a href="https://www.gbif.org/species/${key}" target="_blank">${gbifImage}</a>`;
        } else if (source === 'local') {
            source = fontAwesomeIcon('database');
        }
        if (stored) {
            stored = fontAwesomeIcon('check', 'green');
        } else {
            stored = fontAwesomeIcon('times', 'red');
        }
        let action = (`<button 
                        type="button" 
                        onclick="addNewTaxonToObservedList('${canonicalName}',${key},'${rank}',${taxaId})" 
                        class="btn btn-success">${fontAwesomeIcon('plus')}&nbsp;ADD
                       </button>`);
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

function addNewTaxonToObservedList(name, gbifKey, rank, taxaId = null) {
    let postData = {
        'gbifKey': gbifKey,
        'taxonName': name,
        'rank': rank,
        'taxonGroup': taxonGroupName,
        'taxonGroupId': taxonGroupId,
        'csrfmiddlewaretoken': csrfToken
    };
    let table = $('.find-taxon-table');
    table.hide();
    let loading = $('.find-taxon-loading');
    loading.show();

    if (taxaId) {
        $('#addNewTaxonModal').modal('toggle');
        loading.hide();
        $('#add-taxon-input').val('');

        if (taxaIdList.indexOf(taxaId + '') === -1) {
            renderNewTaxon(taxaId, name);
        } else {
            let div = $('.taxon-table-body').find(`.taxon-id[value="${taxaId}"]`);
            if (div.length) {
                div = div.parent().parent();
                div.scrollHere();
                div.highlight();
            }
        }
        return true;
    }

    $.ajax({
        url: `/api/add-new-taxon/`,
        type: 'POST',
        headers: {"X-CSRFToken": csrfToken},
        data: postData,
        success: function (data) {
            $('#addNewTaxonModal').modal('toggle');
            renderNewTaxon(data['id'], name);
            loading.hide();
            $('#add-taxon-input').val('');
        }
    });
}

// Add new taxon row to the existing taxa list
function renderNewTaxon(taxonId, taxonName) {
    let $container = $('.taxon-table-body');
    taxaIdList.push(taxonId + '');
    let $row = $('<tr>');
    $row.html(
        '<td scope="row" class="taxon-name">' +
        taxonName +
        '<input type="hidden" class="taxon-id" value="' + taxonId + '">' +
        '</td>');
    $row.append(
        '<td>' +
        '<div class="form-check">' +
        '<input class="form-check-input observed" type="checkbox"' +
        ' value="True"' +
        ' name="' + taxonId + '-observed">' +
        ' <label class="form-check-label">' +
        ' </label>' +
        '</div>' +
        '</td>');
    let taxonAbundanceInput = $('<input type="number" min="0.001"' +
        ' name="' + taxonId + '-abundance"' +
        ' class="form-control taxon-abundance"' +
        ' placeholder="0">');
    let tdTaxonAbundance = $('<td>');
    tdTaxonAbundance.append(taxonAbundanceInput);
    $row.append(tdTaxonAbundance);
    $container.prepend($row);
    $row.scrollHere();
    $row.highlight();
    taxonAbundanceInput.change(taxonAbundanceOnChange);
}

function deleteRecord(recordId) {
    $(`#${recordId}`).remove();
    let index = collectionIdList.indexOf(recordId);
    if (index > -1) {
        collectionIdList.splice(index, 1);
    }
}

$('.from-close').click(function (e){
    window.history.back()
});


function updateMultiViewer(input) {
    const container = $('#site-images-viewer');
    const errorEl = $('#site-images-error');
    container.empty();
    errorEl.hide().text('');

    if (!input.files || !input.files.length) return;

    const maxSize = 5 * 1024 * 1024;
    const allowedTypes = ['image/jpeg', 'image/png'];
    const errors = [];

    Array.from(input.files).forEach(function (file) {
        if (!allowedTypes.includes(file.type)) {
            errors.push(file.name + ': only JPEG and PNG are accepted.');
            return;
        }
        if (file.size > maxSize) {
            errors.push(file.name + ': exceeds the 5 MB size limit.');
            return;
        }

        const reader = new FileReader();
        reader.onload = function (e) {
            const card = $(`
                <div class="card" style="width: 180px;">
                    <img src="${e.target.result}" class="card-img-top" style="height:120px;object-fit:cover;">
                    <div class="card-body p-2">
                        <div class="small text-muted">${file.name}</div>
                    </div>
                </div>
            `);
            container.append(card);
        };
        reader.readAsDataURL(file);
    });

    if (errors.length) {
        errorEl.html(errors.join('<br>')).show();
        input.value = '';
        container.empty();
    }
}
