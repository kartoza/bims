/**
 * Chart.js helpers that give charts the look and behaviour of the Highcharts
 * (Highstock) charts they replace: a different marker symbol per series
 * (shown in the legend, the tooltip and on hover), a vertical crosshair, a
 * month based x axis and a navigator ("timeline") under the main chart.
 *
 * Requires Chart.js 4 to be loaded first. Exposes `window.BimsChart`.
 *
 * Usage:
 *   const chart = new Chart(canvas, {
 *       type: 'line',
 *       data: {labels, datasets: [BimsChart.styleDataset({label, data}, color, index)]},
 *       options: {plugins: {legend: BimsChart.legendOptions(),
 *                           tooltip: BimsChart.tooltipOptions({...callbacks})}},
 *       plugins: [BimsChart.crosshairPlugin, BimsChart.whiteBackgroundPlugin],
 *   });
 *   BimsChart.attachNavigator(chart, navigatorCanvas, {labels, tickLabels, data});
 */
(function (global) {
    'use strict';

    // Same order as the default Highcharts symbol cycle:
    // circle, diamond, square, triangle, triangle-down.
    const SYMBOLS = [
        {pointStyle: 'circle', rotation: 0},
        {pointStyle: 'rectRot', rotation: 0},
        {pointStyle: 'rect', rotation: 0},
        {pointStyle: 'triangle', rotation: 0},
        {pointStyle: 'triangle', rotation: 180},
    ];

    function symbolFor(index) {
        return SYMBOLS[index % SYMBOLS.length];
    }

    function drawShape(ctx, symbol, x, y, radius) {
        ctx.beginPath();
        if (symbol.pointStyle === 'circle') {
            ctx.arc(x, y, radius, 0, 2 * Math.PI);
        } else if (symbol.pointStyle === 'rectRot') {
            ctx.moveTo(x, y - radius);
            ctx.lineTo(x + radius, y);
            ctx.lineTo(x, y + radius);
            ctx.lineTo(x - radius, y);
            ctx.closePath();
        } else if (symbol.pointStyle === 'rect') {
            ctx.rect(x - radius + 0.5, y - radius + 0.5, 2 * radius - 1, 2 * radius - 1);
        } else {
            const direction = symbol.rotation === 180 ? -1 : 1;
            ctx.moveTo(x, y - direction * radius);
            ctx.lineTo(x + radius, y + direction * radius * 0.8);
            ctx.lineTo(x - radius, y + direction * radius * 0.8);
            ctx.closePath();
        }
        ctx.fill();
    }

    /**
     * Legend symbol: a short line with the series marker in the middle, or a
     * small square for bar (column) series (kind === 'bar').
     */
    function legendSymbol(color, index, kind) {
        const canvas = document.createElement('canvas');
        canvas.width = 30;
        canvas.height = 16;
        const ctx = canvas.getContext('2d');
        ctx.strokeStyle = color;
        ctx.fillStyle = color;
        if (kind === 'bar') {
            ctx.beginPath();
            if (ctx.roundRect) {
                ctx.roundRect(9, 2, 12, 12, 2);
            } else {
                ctx.rect(9, 2, 12, 12);
            }
            ctx.fill();
            return canvas;
        }
        ctx.lineWidth = 2;
        ctx.beginPath();
        ctx.moveTo(6, 8);
        ctx.lineTo(24, 8);
        ctx.stroke();
        drawShape(ctx, symbolFor(index), 15, 8, 5.5);
        return canvas;
    }

    /** Line dataset styling: no markers until hovered, then the series symbol. */
    function styleDataset(dataset, color, index) {
        const symbol = symbolFor(index);
        return Object.assign(dataset, {
            borderColor: color,
            backgroundColor: color,
            borderWidth: 2,
            pointRadius: 0,
            pointHoverRadius: 5,
            pointHoverBackgroundColor: color,
            pointHoverBorderColor: '#ffffff',
            pointHoverBorderWidth: 1,
            pointStyle: symbol.pointStyle,
            pointRotation: symbol.rotation,
            tension: 0,
            spanGaps: false,
        });
    }

    function legendOptions(position, kind) {
        return {
            position: position || 'top',
            labels: {
                usePointStyle: true,
                // Wider than the 30px symbol so there is a gap before the text.
                boxWidth: 38,
                boxHeight: 16,
                padding: 16,
                generateLabels: chart => Chart.defaults.plugins.legend.labels
                    .generateLabels(chart)
                    .map(item => Object.assign(item, {
                        pointStyle: legendSymbol(
                            kind === 'bar' ? item.fillStyle : item.strokeStyle,
                            item.datasetIndex, kind),
                    })),
            },
        };
    }

    /** Shared tooltip that lists every series at the hovered x, with its symbol. */
    function tooltipOptions(callbacks, kind) {
        return {
            backgroundColor: 'rgba(255, 255, 255, 0.95)',
            titleColor: '#333333',
            bodyColor: '#333333',
            borderColor: '#cccccc',
            borderWidth: 1,
            usePointStyle: true,
            callbacks: Object.assign({
                labelPointStyle: item => (
                    kind === 'bar'
                        ? {pointStyle: 'rect', rotation: 0}
                        : symbolFor(item.datasetIndex)),
            }, callbacks || {}),
        };
    }

    /** Vertical line at the hovered x, like the Highstock crosshair. */
    const crosshairPlugin = {
        id: 'hoverCrosshair',
        afterDatasetsDraw: chart => {
            const active = chart.tooltip && chart.tooltip.getActiveElements();
            if (!active || !active.length) {
                return;
            }
            const x = active[0].element.x;
            const area = chart.chartArea;
            const ctx = chart.ctx;
            ctx.save();
            ctx.strokeStyle = '#cccccc';
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(x, area.top);
            ctx.lineTo(x, area.bottom);
            ctx.stroke();
            ctx.restore();
        },
    };

    /** PNG exports would otherwise have a transparent background. */
    const whiteBackgroundPlugin = {
        id: 'whiteBackground',
        beforeDraw: chart => {
            const ctx = chart.ctx;
            ctx.save();
            ctx.globalCompositeOperation = 'destination-over';
            ctx.fillStyle = '#ffffff';
            ctx.fillRect(0, 0, chart.width, chart.height);
            ctx.restore();
        },
    };

    /**
     * Tick labels for a category axis of daily data: only the first point of
     * each month is labelled ("Jan 24"), the rest are ''. Index them with the
     * tick `value` (the data index), not the tick position.
     */
    function monthTickLabels(labels) {
        const monthIndex = label => {
            const d = new Date(label);
            return d.getFullYear() * 12 + d.getMonth();
        };
        return labels.map((label, i) => (
            i === 0 || monthIndex(label) !== monthIndex(labels[i - 1])
                ? new Date(label).toLocaleDateString('en-GB', {month: 'short', year: '2-digit'})
                : ''
        ));
    }

    /** Category axis options that use monthTickLabels. */
    function monthAxisOptions(tickLabels, extra) {
        return Object.assign({
            ticks: {
                autoSkip: false,
                maxRotation: 0,
                callback: value => tickLabels[value],
            },
            grid: {
                color: ctx => tickLabels[ctx.tick.value] ? '#e6e6e6' : 'transparent',
            },
        }, extra || {});
    }

    /** CSV of a chart's labels and datasets, for the download menu. */
    function toCsv(chart, labelHeader) {
        const quote = value => `"${String(value).replace(/"/g, '""')}"`;
        const rows = [[labelHeader || 'Date']
            .concat(chart.data.datasets.map(d => d.label)).map(quote).join(',')];
        chart.data.labels.forEach((label, i) => {
            rows.push([quote(label)].concat(
                chart.data.datasets.map(d => d.data[i] === null ? '' : d.data[i])
            ).join(','));
        });
        return rows.join('\n');
    }

    /** Save a blob as a file in the browser. */
    function downloadBlob(blob, filename) {
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        setTimeout(() => URL.revokeObjectURL(url), 1000);
    }

    /**
     * Timeline under a category axis chart, like the Highstock navigator: an
     * overview line with a draggable window that sets the visible range of
     * `mainChart`. Drag inside the window to pan, drag its handles to resize
     * it, or drag outside it (or anywhere when everything is visible) to
     * select a new range.
     *
     * options: {labels, data (overview series values), tickLabels?, spanGaps?}.
     * Without tickLabels the axis labels are skipped automatically; spanGaps
     * overrides whether the overview line connects across missing values.
     * Returns {applyRange(start, end), showLast(points), chart}.
     */
    function attachNavigator(mainChart, canvas, options) {
        const labels = options.labels;
        const total = labels.length;
        let range = {start: 0, end: total - 1};
        let navigator = null;

        const applyRange = (start, end) => {
            const minSpan = Math.min(5, total - 1);
            start = Math.max(0, Math.min(start, total - 1 - minSpan));
            end = Math.min(total - 1, Math.max(end, start + minSpan));
            range = {start: start, end: end};
            mainChart.options.scales.x.min = start;
            mainChart.options.scales.x.max = end;
            mainChart.update('none');
            navigator.update('none');
        };

        navigator = new Chart(canvas, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    data: options.data,
                    borderColor: '#000000',
                    borderWidth: 1,
                    pointRadius: 0,
                    tension: 0,
                    // Defaults to the main chart's gap handling, so the overview
                    // line breaks (or not) exactly where the chart's does.
                    spanGaps: options.spanGaps !== undefined
                        ? options.spanGaps
                        : (mainChart.data.datasets.length > 0
                            && mainChart.data.datasets[0].spanGaps === true),
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                events: [],
                scales: {
                    x: {
                        ticks: options.tickLabels ? {
                            autoSkip: false,
                            maxRotation: 0,
                            font: {size: 10},
                            callback: value => options.tickLabels[value],
                        } : {
                            autoSkip: true,
                            maxTicksLimit: 8,
                            maxRotation: 0,
                            font: {size: 10},
                        },
                        grid: {display: false},
                    },
                    y: {display: false},
                },
                plugins: {legend: {display: false}, tooltip: {enabled: false}},
            },
            plugins: [{
                // Shades everything outside the visible range and draws its handles.
                id: 'navigatorWindow',
                afterDraw: chart => {
                    const ctx = chart.ctx;
                    const area = chart.chartArea;
                    const left = chart.scales.x.getPixelForValue(range.start);
                    const right = chart.scales.x.getPixelForValue(range.end);
                    ctx.save();
                    ctx.fillStyle = 'rgba(102, 133, 194, 0.15)';
                    ctx.fillRect(area.left, area.top, left - area.left, area.height);
                    ctx.fillRect(right, area.top, area.right - right, area.height);
                    ctx.strokeStyle = '#9aa5b8';
                    ctx.lineWidth = 1;
                    ctx.strokeRect(left, area.top, right - left, area.height);
                    ctx.fillStyle = '#f2f2f2';
                    [left, right].forEach(x => {
                        const y = area.top + area.height / 2 - 10;
                        ctx.fillRect(x - 3, y, 6, 20);
                        ctx.strokeRect(x - 3, y, 6, 20);
                    });
                    ctx.restore();
                },
            }],
        });

        // Line the timeline up with the plot area of the main chart.
        const align = () => {
            navigator.options.layout = {padding: {
                left: mainChart.chartArea.left,
                right: mainChart.width - mainChart.chartArea.right,
            }};
            navigator.update('none');
        };
        mainChart.options.onResize = align;
        align();

        const HANDLE_GRAB = 8;
        const pixelX = event => event.clientX - canvas.getBoundingClientRect().left;
        const pointerIndex = event => Math.round(
            navigator.scales.x.getValueForPixel(pixelX(event)));
        const hitTest = event => {
            const x = pixelX(event);
            const left = navigator.scales.x.getPixelForValue(range.start);
            const right = navigator.scales.x.getPixelForValue(range.end);
            if (Math.abs(x - left) <= HANDLE_GRAB) return 'start';
            if (Math.abs(x - right) <= HANDLE_GRAB) return 'end';
            const isFullRange = range.start === 0 && range.end === total - 1;
            if (!isFullRange && x > left && x < right) return 'move';
            return 'new';
        };

        let drag = null;
        canvas.addEventListener('pointerdown', event => {
            canvas.setPointerCapture(event.pointerId);
            drag = {mode: hitTest(event), origin: pointerIndex(event), range: Object.assign({}, range)};
        });
        canvas.addEventListener('pointermove', event => {
            if (!drag) {
                const mode = hitTest(event);
                canvas.style.cursor = (mode === 'start' || mode === 'end')
                    ? 'ew-resize' : (mode === 'move' ? 'grab' : 'crosshair');
                return;
            }
            const delta = pointerIndex(event) - drag.origin;
            if (drag.mode === 'start') {
                applyRange(drag.range.start + delta, drag.range.end);
            } else if (drag.mode === 'end') {
                applyRange(drag.range.start, drag.range.end + delta);
            } else if (drag.mode === 'move') {
                const span = drag.range.end - drag.range.start;
                const start = Math.max(0, Math.min(drag.range.start + delta, total - 1 - span));
                applyRange(start, start + span);
            } else {
                const index = pointerIndex(event);
                applyRange(Math.min(drag.origin, index), Math.max(drag.origin, index));
            }
        });
        const endDrag = () => { drag = null; };
        canvas.addEventListener('pointerup', endDrag);
        canvas.addEventListener('pointercancel', endDrag);

        return {
            chart: navigator,
            applyRange: applyRange,
            // Show the last `points` data points, or everything when 0.
            showLast: points => applyRange(points ? total - 1 - points : 0, total - 1),
        };
    }

    global.BimsChart = {
        SYMBOLS: SYMBOLS,
        symbolFor: symbolFor,
        legendSymbol: legendSymbol,
        styleDataset: styleDataset,
        legendOptions: legendOptions,
        tooltipOptions: tooltipOptions,
        crosshairPlugin: crosshairPlugin,
        whiteBackgroundPlugin: whiteBackgroundPlugin,
        monthTickLabels: monthTickLabels,
        monthAxisOptions: monthAxisOptions,
        toCsv: toCsv,
        downloadBlob: downloadBlob,
        attachNavigator: attachNavigator,
    };
})(window);
