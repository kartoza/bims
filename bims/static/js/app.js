require.config({
    paths: {
        jquery: 'libs/jquery/jquery-3.3.1.min',
        underscore: 'libs/underscore-1.8.3/underscore-min',
        backbone: 'libs/backbone-1.3.3/backbone-min',
        bootstrap: 'libs/bootstrap-4.0.0/js/bootstrap.bundle.min',
        jqueryUi: 'libs/jquery-ui-1.12.1/jquery-ui.min',
        select2: 'libs/select2/select2.min',
        layerSwitcher: 'libs/ol-layerswitcher-4.1.2/ol-layerswitcher',
        // olMapboxStyle: 'libs/ol-mapbox-style/olms', // upgrade this
        noUiSlider: 'libs/noUiSlider.11.1.0/nouislider',
        chartJs: 'libs/chart/Chart-2.7.2',
        fileSaver: 'libs/FileSaver.js/1.3.3/FileSaver.min',
        htmlToCanvas: 'libs/htmlToCanvas-1.4.1/html2canvas.min',
        jspdf: 'libs/jspdf.umd.min',
        gridStack: 'libs/gridstack-1.1.2/gridstack.min',
        chosen: 'libs/chosen/chosen.jquery.min',
        detectBrowser: 'utils/detect-browser',
        'jquery-ui/ui/widget': 'libs/jquery-fileupload/vendor/jquery.ui.widget',
        'jquery.iframe-transport': 'libs/jquery-fileupload/jquery.iframe-transport',
        'jquery.fileupload': 'libs/jquery-fileupload/jquery.fileupload',
        'jquery.fileupload-process': 'libs/jquery-fileupload/jquery.fileupload-process',
        'jquery.fileupload-validate': 'libs/jquery-fileupload/jquery.fileupload-validate',
    },
    shim: {
        underscore: {
            exports: '_'
        },
        select2: {
            exports: 'select2'
        },
        backbone: {
            deps: [
                'underscore',
                'bootstrap',
                'jqueryUi',
                'jquery',
                'select2'
            ],
            exports: 'Backbone'
        },
        app: {
            deps: ['backbone']
        },
        gridStack: {
            deps: [
                'underscore',
                'jqueryUi',
                'jquery'
            ]
        },
        jspdf: {
            exports: 'jsPDF'
        }
    }
});

require([
    'router',
    'views/olmap',
    'shared',
    'app',
    'bootstrap',
    'jqueryUi',
    'jquery'
], function (Router, olmap, Shared, App, Bootstrap, jqueryUi, $) {
    // Display the map
    Shared.Router = new Router();

    // Start Backbone history a necessary step for bookmarkable URL's
    Backbone.history.start({hashChange: true, root: "/map/"});

    // A $( document ).ready() block.
    $(document).ready(function () {
        $("#conservation-status").chosen();
        $('#menu-dropdown-burger').click(function () {
            $('.dropdown-menu-left').toggle();
        });

        $('#menu-dropdown-account').click(function () {
            $('.right-nav-dropdown').toggle();
        });

        $('[data-toggle="tooltip"]').tooltip();
        $('[data-toggle="popover"]').popover();

        $('#native-origin-btn').popover({
            content: 'Native: (or indigenous) means a taxon occurring within its natural ' +
                'range (past or present) and dispersal potential ' +
                '(i.e. within the range it occupies naturally or' +
                ' could occupy without direct or indirect introduction or care by humans)',
            trigger: 'hover',
            placement: 'top',
        });

        $('#non-native-origin-btn').popover({
            content: 'Non-native: A category that includes both Alien and Extralimital taxa',
            trigger: 'hover',
            placement: 'top',
        });

        $('#alien-origin-checkbox').popover({
            content: 'Alien: (non-native, non-indigenous, foreign, exotic) means a taxon ' +
                'occurring outside of its natural range (past or present) and dispersal ' +
                'potential (i.e. outside the range it occupies naturally or could not occupy ' +
                'without direct or indirect introduction or care by humans) and includes any part, ' +
                'gametes or propagule of such species that might survive and subsequently reproduce',
            trigger: 'hover',
            placement: 'top',
        });

        $('#extralimital-origin-checkbox').popover({
            content: 'Extralimital: Species native to South Africa that have been ' +
                'translocated into areas where they did not naturally occur.',
            trigger: 'hover',
            placement: 'top',
        });

        $('.try-again-button').click(function () {
            Shared.Dispatcher.trigger('map:reloadXHR', this.features)
        });
        $('.mouse-position button').click(function () {
            if ($('.mouse-position').hasClass('active')) {
                $('.mouse-position').removeClass('active');
                $('#mouse-position-wrapper').hide();
            } else {
                $('.mouse-position').addClass('active');
                $('#mouse-position-wrapper').show();
            }
        });
        $(".date-filter").datepicker({dateFormat: 'yy-mm-dd'});
        $('.close-info-popup').click(function () {
            if($('input[name=dont-show-info]').is(':checked')){
                $.ajax({
                    type: 'GET',
                    url: hideBimsInfoUrl,
                    success: function () {
                        $('#general-info-modal').fadeOut();
                    }
                })
            }
            $('#general-info-modal').fadeOut();
        });

        // Show/hide climate module option based on feature flag
        if (typeof is_climate_enabled !== 'undefined' && is_climate_enabled) {
            $('#climate-module-wrapper').show();
        }
        if (typeof is_groundwater_enabled !== 'undefined' && is_groundwater_enabled) {
            $('#groundwater-module-wrapper').show();
        }

        showSiteNotice();
    });
});
