var metadataSourceReferences = [];

function setMetadataSourceReferences(dataSources) {
    metadataSourceReferences = dataSources || [];
}

function downloadCitationList(e) {
    e.preventDefault();
    let format = $(e.currentTarget).data('format');
    let alertModalBody = $('#alertModalBody');

    if (!is_logged_in) {
        alertModalBody.html('Please log in first.');
        $('#alertModal').modal({'keyboard': false, 'backdrop': 'static'});
        return;
    }

    let sourceReferenceIds = metadataSourceReferences
        .filter(function (s) { return s['ID'] && s['Reference Category'] !== 'Occurrence dataset'; })
        .map(function (s) { return s['ID']; });
    let datasetIds = metadataSourceReferences
        .filter(function (s) { return s['ID'] && s['Reference Category'] === 'Occurrence dataset'; })
        .map(function (s) { return s['ID']; });

    if (sourceReferenceIds.length === 0 && datasetIds.length === 0) {
        alertModalBody.html('No source references available to download.');
        $('#alertModal').modal({'keyboard': false, 'backdrop': 'static'});
        return;
    }

    showDownloadPopup('CSV', 'Citation List', function (downloadRequestId) {
        let formData = new FormData();
        formData.append('citation_format', format);
        formData.append('download_request_id', downloadRequestId);
        $.each(sourceReferenceIds, function (i, id) {
            formData.append('source_reference_ids', id);
        });
        $.each(datasetIds, function (i, id) {
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
}

$(function () {
    $(document).on('click', '.download-citation', downloadCitationList);
});

function renderSourceReferences() {
    let divWrapper = $('#data-source-list');
    let dataSources = sourceReferences;
    setMetadataSourceReferences(dataSources);
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
    divWrapper.append(headerDiv);

    var bodyDiv = $('<tbody></tbody>');
    $.each(orderedDataSources, function (index, source) {
        var itemDiv = $('<tr></tr>');
        var keys = Object.keys(source);
        var document = false;
        for(var i=0; i<keys.length; i++){
            if(source[keys[i]] === 'Published book, report or thesis'){
                document = true
            }

            if(keys[i] === 'DOI/URL' && document){
                itemDiv.append('<td><a href="'+ source[keys[i]] + '" target="_blank">Download</a></td>')
            }else {
                if (source[keys[i]]) {
                    itemDiv.append('<td>' + source[keys[i]] + '</td>')
                }
            }
        }
        bodyDiv.append(itemDiv);
    });
    divWrapper.append(bodyDiv);
}