/**
 * charts.js — Highcharts rendering, all charts reactive to filter state.
 * Depends on: api.js (loaded first), Highcharts (CDN).
 */

// ---------------------------------------------------------------------------
// Shared Highcharts theme
// ---------------------------------------------------------------------------
Highcharts.setOptions({
    chart: {
        style: { fontFamily: "'Inter', 'Segoe UI', Arial, sans-serif" },
        backgroundColor: "#ffffff",
        borderRadius: 0,
    },
    colors: ["#2563EB", "#10B981", "#F59E0B", "#EF4444", "#8B5CF6",
             "#06B6D4", "#F97316", "#EC4899", "#6366F1", "#14B8A6"],
    title:    { style: { fontSize: "14px", fontWeight: "600", color: "#1e293b" } },
    subtitle: { style: { fontSize: "11px", color: "#64748b" } },
    xAxis:    { labels: { style: { color: "#64748b", fontSize: "11px" } } },
    yAxis: {
        labels: { style: { color: "#64748b", fontSize: "11px" } },
        gridLineColor: "#f1f5f9",
        title: { style: { color: "#94a3b8", fontSize: "11px" } }
    },
    legend:   { itemStyle: { color: "#334155", fontWeight: "500", fontSize: "12px" } },
    tooltip:  { backgroundColor: "#1e293b", style: { color: "#f8fafc", fontSize: "12px" },
                borderRadius: 8, shadow: false, borderWidth: 0 },
    credits:  { enabled: false },
    plotOptions: { series: { animation: { duration: 350 } } }
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function _fmt(value) {
    if (value == null || isNaN(value)) return "$0";
    if (value >= 1_000_000) return `$${(value / 1_000_000).toFixed(2)}M`;
    if (value >= 1_000)     return `$${(value / 1_000).toFixed(1)}K`;
    return `$${value.toFixed(2)}`;
}

function _tc(str) {
    if (!str) return "";
    return str.replace(/\b\w/g, c => c.toUpperCase());
}

function _subtitle() {
    // Use filterLabel() from ui-shared.js if available
    return typeof filterLabel === "function" ? filterLabel() : "";
}

function _placeholder(id, msg = "Loading…") {
    const e = document.getElementById(id);
    if (e) e.innerHTML = `<div class="chart-loading">${msg}</div>`;
}

function _error(id) {
    const e = document.getElementById(id);
    if (e) e.innerHTML = `<div class="chart-error">Failed to load</div>`;
}

// ---------------------------------------------------------------------------
// Revenue by Category — Column
// ---------------------------------------------------------------------------
async function renderCategoryChart(filters = {}) {
    if (!document.getElementById("categoryChart")) return;
    _placeholder("categoryChart");
    try {
        const data   = await getRevenueByCategory(filters);
        const labels = Object.keys(data).map(_tc);
        const values = Object.values(data);

        Highcharts.chart("categoryChart", {
            chart: { type: "column" },
            title:    { text: "Revenue by Category" },
            subtitle: { text: _subtitle() },
            xAxis: { categories: labels, crosshair: true },
            yAxis: {
                title: { text: "Revenue (USD)" },
                labels: { formatter() { return _fmt(this.value); } }
            },
            tooltip: {
                formatter() { return `<b>${this.x}</b><br/>Revenue: <b>${_fmt(this.y)}</b>`; }
            },
            series: [{ name: "Revenue", data: values, colorByPoint: true }],
            legend: { enabled: false }
        });
    } catch { _error("categoryChart"); }
}

// ---------------------------------------------------------------------------
// Monthly Revenue Trend — Area
// ---------------------------------------------------------------------------
async function renderMonthlyChart(filters = {}) {
    if (!document.getElementById("monthlyChart")) return;
    _placeholder("monthlyChart");
    try {
        const data   = await getMonthlyRevenue(filters);
        const labels = Object.keys(data);
        const values = Object.values(data);

        Highcharts.chart("monthlyChart", {
            chart: { type: "area" },
            title:    { text: "Monthly Revenue Trend" },
            subtitle: { text: _subtitle() },
            xAxis: {
                categories: labels,
                tickInterval: Math.max(1, Math.ceil(labels.length / 14)),
                labels: { rotation: -45, style: { fontSize: "10px" } }
            },
            yAxis: {
                title: { text: "Revenue (USD)" },
                labels: { formatter() { return _fmt(this.value); } }
            },
            tooltip: {
                formatter() { return `<b>${this.x}</b><br/>Revenue: <b>${_fmt(this.y)}</b>`; }
            },
            plotOptions: {
                area: {
                    fillOpacity: 0.12,
                    marker: { enabled: false, states: { hover: { enabled: true, radius: 4 } } }
                }
            },
            series: [{ name: "Revenue", data: values, color: "#2563EB" }],
            legend: { enabled: false }
        });
    } catch { _error("monthlyChart"); }
}

// ---------------------------------------------------------------------------
// Top Brands — Horizontal Bar
// ---------------------------------------------------------------------------
async function renderBrandChart(filters = {}) {
    if (!document.getElementById("brandChart")) return;
    _placeholder("brandChart");
    try {
        const data   = await getRevenueByBrand(filters);
        const sorted = Object.entries(data).sort((a, b) => b[1] - a[1]).slice(0, 10);
        const labels = sorted.map(([k]) => _tc(k));
        const values = sorted.map(([, v]) => v);

        Highcharts.chart("brandChart", {
            chart: { type: "bar" },
            title:    { text: "Top Brands by Revenue" },
            subtitle: { text: _subtitle() },
            xAxis: { categories: labels },
            yAxis: {
                title: { text: "Revenue (USD)" },
                labels: { formatter() { return _fmt(this.value); } }
            },
            tooltip: {
                formatter() { return `<b>${this.x}</b><br/>Revenue: <b>${_fmt(this.y)}</b>`; }
            },
            series: [{ name: "Revenue", data: values, color: "#10B981" }],
            legend: { enabled: false }
        });
    } catch { _error("brandChart"); }
}

// ---------------------------------------------------------------------------
// Revenue by Region — Horizontal bar (replaces pie — better comparison)
// ---------------------------------------------------------------------------
async function renderRegionChart(filters = {}) {
    if (!document.getElementById("regionChart")) return;
    _placeholder("regionChart");
    try {
        const data   = await getRevenueByRegion(filters);
        const sorted = Object.entries(data).sort((a, b) => b[1] - a[1]);
        const labels = sorted.map(([k]) => _tc(k));
        const values = sorted.map(([, v]) => v);

        Highcharts.chart("regionChart", {
            chart: { type: "bar" },
            title:    { text: "Revenue by Region" },
            subtitle: { text: _subtitle() },
            xAxis: { categories: labels },
            yAxis: {
                title: { text: "Revenue (USD)" },
                labels: { formatter() { return _fmt(this.value); } }
            },
            tooltip: {
                formatter() {
                    const total = values.reduce((s, v) => s + v, 0);
                    const pct   = total > 0 ? (this.y / total * 100).toFixed(1) : 0;
                    return `<b>${this.x}</b><br/>Revenue: <b>${_fmt(this.y)}</b><br/>Share: <b>${pct}%</b>`;
                }
            },
            plotOptions: {
                bar: {
                    dataLabels: {
                        enabled: true,
                        formatter() { return _fmt(this.y); },
                        style: { fontSize: "10px", color: "#475569", fontWeight: "500" }
                    }
                }
            },
            series: [{ name: "Revenue", data: values, colorByPoint: true }],
            legend: { enabled: false }
        });
    } catch { _error("regionChart"); }
}

// ---------------------------------------------------------------------------
// Revenue by Customer Segment — Column
// ---------------------------------------------------------------------------
async function renderSegmentChart(filters = {}) {
    if (!document.getElementById("segmentChart")) return;
    _placeholder("segmentChart");
    try {
        const data   = await getRevenueBySegment(filters);
        const sorted = Object.entries(data).sort((a, b) => b[1] - a[1]);
        const labels = sorted.map(([k]) => _tc(k));
        const values = sorted.map(([, v]) => v);

        Highcharts.chart("segmentChart", {
            chart: { type: "column" },
            title:    { text: "Revenue by Customer Segment" },
            subtitle: { text: _subtitle() },
            xAxis: { categories: labels, crosshair: true },
            yAxis: {
                title: { text: "Revenue (USD)" },
                labels: { formatter() { return _fmt(this.value); } }
            },
            tooltip: {
                formatter() { return `<b>${this.x}</b><br/>Revenue: <b>${_fmt(this.y)}</b>`; }
            },
            series: [{ name: "Revenue", data: values, colorByPoint: true }],
            legend: { enabled: false }
        });
    } catch { _error("segmentChart"); }
}

// ---------------------------------------------------------------------------
// Top 10 Products — Horizontal Bar (full width)
// ---------------------------------------------------------------------------
async function renderTopProductsChart(filters = {}) {
    if (!document.getElementById("topProductsChart")) return;
    _placeholder("topProductsChart");
    try {
        const data   = await getTopProducts(filters, 10);
        const sorted = Object.entries(data).sort((a, b) => b[1] - a[1]);
        const labels = sorted.map(([k]) => k.length > 40 ? k.slice(0, 37) + "…" : k);
        const values = sorted.map(([, v]) => v);

        Highcharts.chart("topProductsChart", {
            chart: { type: "bar", height: 400 },
            title:    { text: "Top 10 Products by Revenue" },
            subtitle: { text: _subtitle() },
            xAxis: {
                categories: labels,
                labels: { style: { fontSize: "11px", color: "#475569" } }
            },
            yAxis: {
                title: { text: "Revenue (USD)" },
                labels: { formatter() { return _fmt(this.value); } }
            },
            tooltip: {
                formatter() { return `<b>${this.x}</b><br/>Revenue: <b>${_fmt(this.y)}</b>`; }
            },
            plotOptions: {
                bar: {
                    dataLabels: {
                        enabled: true,
                        formatter() { return _fmt(this.y); },
                        style: { fontSize: "10px", color: "#475569", fontWeight: "500" }
                    }
                }
            },
            series: [{ name: "Revenue", data: values, color: "#8B5CF6" }],
            legend: { enabled: false }
        });
    } catch { _error("topProductsChart"); }
}

// ---------------------------------------------------------------------------
// Refresh all charts on summary page
// ---------------------------------------------------------------------------
async function refreshAllCharts(filters = {}) {
    await Promise.all([
        renderCategoryChart(filters),
        renderMonthlyChart(filters),
        renderBrandChart(filters),
        renderRegionChart(filters),
        renderSegmentChart(filters),
        renderTopProductsChart(filters),
    ]);
}
