/**
 * summary.js — Summary page logic.
 * Depends on: api.js, charts.js, ui-shared.js
 */

// ---------------------------------------------------------------------------
// Summary cards
// ---------------------------------------------------------------------------
async function updateSummaryCards() {
    try {
        const s = await getSummary(currentFilters);
        const set = (id, v) => { const e = el(id); if (e) e.textContent = v; };
        set("stat-revenue",      fmtMoney(s.total_revenue));
        set("stat-transactions", fmtNumber(s.total_transactions));
        set("stat-avg-order",    fmtMoney(s.avg_order_value));
        set("stat-refunds",      `${fmtNumber(s.total_refunds)} (${s.refund_rate}%)`);
    } catch (e) {
        console.warn("Summary cards failed:", e);
    }
}

// ---------------------------------------------------------------------------
// Refresh everything
// ---------------------------------------------------------------------------
async function refreshSummaryPage() {
    showLoading();
    try {
        await Promise.all([
            updateSummaryCards(),
            refreshAllCharts(currentFilters),
        ]);
    } finally {
        hideLoading();
    }
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
document.addEventListener("DOMContentLoaded", async () => {
    await initFilters(refreshSummaryPage);
    readFilters();
    await refreshSummaryPage();
});
