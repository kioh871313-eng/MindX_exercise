/**
 * transactions.js — Transactions page logic.
 * Depends on: api.js, ui-shared.js
 */

const PAGE_LIMIT = 100;
let currentPage  = 0;
let totalRows    = 0;

// ---------------------------------------------------------------------------
// Table rendering
// ---------------------------------------------------------------------------
function updateTable(response) {
    const tbody = el("data-table-body");
    if (!tbody) return;

    const rows  = response?.data ?? [];
    totalRows   = response?.total ?? rows.length;

    tbody.innerHTML = "";

    if (!rows.length) {
        tbody.innerHTML =
            `<tr><td colspan="11" class="empty-row">No records found.</td></tr>`;
        updatePaginationUI();
        return;
    }

    const frag = document.createDocumentFragment();
    rows.forEach(row => {
        const tr = document.createElement("tr");
        tr.classList.add("clickable-row");
        tr.dataset.row = JSON.stringify(row);
        tr.innerHTML = `
            <td class="mono">${row.Transaction_ID ?? "—"}</td>
            <td>${row.Transaction_Date ?? "—"}</td>
            <td><span class="badge">${row.Product_Category ?? "—"}</span></td>
            <td>${_tc(row.Brand)}</td>
            <td class="col-product" title="${row.Product_Name ?? ""}">${row.Product_Name ?? "—"}</td>
            <td class="num">$${_n2(row.Unit_Price_USD)}</td>
            <td class="num">${row.Quantity ?? "—"}</td>
            <td class="num fw-600">$${_n2(row.Total_Amount_USD)}</td>
            <td>${_tc(row.Region)}</td>
            <td>${_tc(row.Payment_Method)}</td>
            <td class="refund-cell">${row.Is_Refund === true
                ? '<span class="badge badge--red">Yes</span>'
                : '<span class="badge badge--green">No</span>'}</td>
        `;
        tr.addEventListener("click", () => openModal(row));
        frag.appendChild(tr);
    });
    tbody.appendChild(frag);
    updatePaginationUI();
}

function _tc(v) {
    if (!v) return "—";
    return v.replace(/\b\w/g, c => c.toUpperCase());
}

function _n2(v) {
    return (v != null && !isNaN(v)) ? Number(v).toFixed(2) : "—";
}

function updatePaginationUI() {
    const offset = currentPage * PAGE_LIMIT;
    const end    = Math.min(offset + PAGE_LIMIT, totalRows);
    const label  = totalRows
        ? `Showing ${fmtNumber(offset + 1)}–${fmtNumber(end)} of ${fmtNumber(totalRows)} records`
        : "No records";

    ["pagination-info", "pagination-info-bottom"].forEach(id => {
        const e = el(id); if (e) e.textContent = label;
    });

    const prev = el("btn-prev");
    const next = el("btn-next");
    if (prev) prev.disabled = currentPage === 0;
    if (next) next.disabled = end >= totalRows;
}

// ---------------------------------------------------------------------------
// Fetch + render table for current page
// ---------------------------------------------------------------------------
async function loadTablePage(page = 0) {
    currentPage = page;
    showLoading();
    try {
        const res = await fetchCombined({
            ...currentFilters,
            limit:  PAGE_LIMIT,
            offset: page * PAGE_LIMIT,
        });
        updateTable(res);
    } catch (e) {
        console.error("loadTablePage failed:", e);
        const tbody = el("data-table-body");
        if (tbody) tbody.innerHTML =
            `<tr><td colspan="11" class="empty-row" style="color:#ef4444">
                Error: ${e.message}<br>
                <small style="color:#94a3b8">Check browser Console (F12) and confirm Flask API is running at http://127.0.0.1:5000</small>
            </td></tr>`;
    } finally {
        hideLoading();
    }
}

// ---------------------------------------------------------------------------
// Transaction detail modal
// ---------------------------------------------------------------------------
function openModal(row) {
    const modal = el("detail-modal");
    const body  = el("modal-body");
    const title = el("modal-title");
    if (!modal || !body) return;

    if (title) title.textContent = `Transaction — ${row.Transaction_ID ?? ""}`;

    const fields = [
        ["Transaction ID",  row.Transaction_ID],
        ["Date",            row.Transaction_Date],
        ["Product",         row.Product_Name],
        ["Category",        _tc(row.Product_Category)],
        ["Brand",           _tc(row.Brand)],
        ["Unit Price",      `$${_n2(row.Unit_Price_USD)}`],
        ["Quantity",        row.Quantity ?? "—"],
        ["Total Amount",    `$${_n2(row.Total_Amount_USD)}`],
        ["Customer Segment",_tc(row.Customer_Segment)],
        ["Region",          _tc(row.Region)],
        ["Payment Method",  _tc(row.Payment_Method)],
        ["Is Refund",       row.Is_Refund === true ? "Yes" : row.Is_Refund === false ? "No" : "—"],
    ];

    body.innerHTML = `
        <dl class="detail-grid">
            ${fields.map(([k, v]) => `
                <dt>${k}</dt>
                <dd>${v ?? "—"}</dd>
            `).join("")}
        </dl>
    `;

    modal.classList.remove("hidden");
    document.body.style.overflow = "hidden";
}

function closeModal() {
    el("detail-modal")?.classList.add("hidden");
    document.body.style.overflow = "";
}

// ---------------------------------------------------------------------------
// Pagination buttons
// ---------------------------------------------------------------------------
function bindPagination() {
    el("btn-prev")?.addEventListener("click", () => {
        if (currentPage > 0) loadTablePage(currentPage - 1);
    });
    el("btn-next")?.addEventListener("click", () => {
        loadTablePage(currentPage + 1);
    });
}

// ---------------------------------------------------------------------------
// Modal events
// ---------------------------------------------------------------------------
function bindModal() {
    el("modal-close")?.addEventListener("click", closeModal);
    el("modal-backdrop")?.addEventListener("click", closeModal);
    document.addEventListener("keydown", e => {
        if (e.key === "Escape") closeModal();
    });
}

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
document.addEventListener("DOMContentLoaded", async () => {
    bindPagination();
    bindModal();

    await initFilters(() => loadTablePage(0));
    readFilters();
    await loadTablePage(0);
});
