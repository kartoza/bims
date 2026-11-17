// Close button of the groundwater dashboards.
//
// Goes back in the browser history to the page the dashboard was opened
// from, so the map comes back with its search, filters and view (the
// part of the map URL after "#" is not in document.referrer). When the
// dashboard has no previous page in this tab, for example when it was
// opened from a link in a new tab, the map page is opened instead.
$(function () {
    var fallbackUrl = '/map/';
    var storageKey = 'groundwaterDashboardClose:' + window.location.pathname;

    function sameOriginPath(url) {
        try {
            var parsed = new URL(url);
            return parsed.origin === window.location.origin ?
                parsed.pathname : null;
        } catch (e) {
            return null;
        }
    }

    function readStored() {
        try {
            return JSON.parse(window.sessionStorage.getItem(storageKey));
        } catch (e) {
            return null;
        }
    }

    function store(info) {
        try {
            window.sessionStorage.setItem(storageKey, JSON.stringify(info));
        } catch (e) {
            // Storage can be blocked, the count then restarts at this page
        }
    }

    // Number of history entries to go back to reach the page before the
    // dashboard. Applying the date filter loads this page again as a new
    // history entry, so those entries are counted too. The count is kept
    // on the history entry, so it stays right after Back or a reload.
    function closeInfo() {
        var state = window.history.state || {};
        if (state.groundwaterClose) {
            return state.groundwaterClose;
        }
        var info = {steps: 0, hasPrevious: false};
        var referrerPath = sameOriginPath(document.referrer);
        if (referrerPath === window.location.pathname) {
            var previous = readStored();
            if (previous && previous.hasPrevious) {
                info = {steps: previous.steps + 1, hasPrevious: true};
            }
        } else if (referrerPath !== null) {
            info = {steps: 1, hasPrevious: true};
        }
        try {
            state.groundwaterClose = info;
            window.history.replaceState(state, '');
        } catch (e) {
            // Without history state the stored count is still used
        }
        return info;
    }

    var info = closeInfo();
    store(info);

    $('.dashboard-close').click(function (e) {
        e.preventDefault();
        if (info.hasPrevious && window.history.length > info.steps) {
            window.history.go(-info.steps);
        } else {
            window.location.href = fallbackUrl;
        }
    });
});
