// Close button of the groundwater dashboards: go back to the page the
// dashboard was opened from when it is on the same site, otherwise to the map.
$(function () {
    var storageKey = 'groundwaterDashboardPrev:' + window.location.pathname;
    var fallbackUrl = '/map/';

    function isSameOrigin(url) {
        try {
            return new URL(url).origin === window.location.origin;
        } catch (e) {
            return false;
        }
    }

    function readStored() {
        try {
            return window.sessionStorage.getItem(storageKey);
        } catch (e) {
            return null;
        }
    }

    function store(url) {
        try {
            window.sessionStorage.setItem(storageKey, url);
        } catch (e) {
            // Storage can be blocked, the map is used as the fallback
        }
    }

    // Applying the date filter reloads this page, so the referrer is then
    // the dashboard itself. Only remember referrers that are other pages.
    var referrer = document.referrer;
    if (referrer && isSameOrigin(referrer) &&
        new URL(referrer).pathname !== window.location.pathname) {
        store(referrer);
    }

    $('.dashboard-close').click(function () {
        var previous = readStored();
        window.location.href = (previous && isSameOrigin(previous)) ?
            previous : fallbackUrl;
        return false;
    });
});
