/* StackPulse Interactive Trace DAG, SRE SLO Burn-Rate & Aegis G4 Studio */

function esc(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function showToast(msg) {
  const el = document.getElementById("toast");
  if (!el) return;
  el.textContent = msg;
  el.classList.add("show");
  clearTimeout(window.__toastTimer);
  window.__toastTimer = setTimeout(() => el.classList.remove("show"), 2200);
}

function fallbackCopy(text) {
  const ta = document.createElement("textarea");
  ta.value = text;
  ta.style.position = "fixed";
  ta.style.opacity = "0";
  document.body.appendChild(ta);
  ta.select();
  try {
    const ok = document.execCommand("copy");
    showToast(ok ? "Copied to clipboard" : "Copy unavailable in this browser");
  } catch (err) {
    showToast("Copy failed: " + (err && err.message ? err.message : "clipboard blocked"));
  }
  document.body.removeChild(ta);
}

function copyText(text) {
  if (!text) return;
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text).then(
      () => showToast("Copied to clipboard"),
      () => fallbackCopy(text)
    );
  } else {
    fallbackCopy(text);
  }
}

document.addEventListener("click", (e) => {
  const copyBtn = e.target.closest("[data-copy]");
  if (copyBtn) {
    copyText(copyBtn.getAttribute("data-copy"));
    return;
  }
  const popBtn = e.target.closest("[data-popover-toggle]");
  if (popBtn) {
    const pop = document.getElementById(popBtn.getAttribute("data-popover-toggle"));
    if (pop) pop.classList.toggle("open");
    return;
  }
  document.querySelectorAll(".cta-popover.open").forEach((pop) => {
    if (!pop.closest(".cta-wrap")?.contains(e.target)) pop.classList.remove("open");
  });
});

const TRACE_SCENARIOS = {
  normal: {
    name: "Normal Baseline Checkout Trace",
    spans: 6,
    totalMs: 44.2,
    exclusiveBottleneck: "auth-service validate_jwt (12.4 ms)",
    anomaly: null,
    errorRatePct: 0.0,
    burnRate1h: 0.0,
    sloStatus: "OK",
    summary: "Healthy fan-out across api-gateway, auth-service, and redis-cache with full error budget remaining."
  },
  n_plus_one: {
    name: "ORM Lazy-Loading N+1 Query Regression",
    spans: 10,
    totalMs: 318.6,
    exclusiveBottleneck: "checkout-service -> postgres-primary SELECT line_items (x7 sequential, 241.0 ms)",
    anomaly: "N_PLUS_ONE_ORM_REGRESSION (>=4 identical sequential DB spans)",
    errorRatePct: 0.4,
    burnRate1h: 4.0,
    sloStatus: "TICKET_WARNING",
    summary: "TraceAnalyzer flagged 7 identical sequential ORM queries under checkout-service; batch prefetching recovers 75% latency."
  },
  retry_storm: {
    name: "Downstream Payment HTTP 503 Retry Storm",
    spans: 8,
    totalMs: 912.5,
    exclusiveBottleneck: "checkout-service -> payment-worker charge_card (5 HTTP 503 retries)",
    anomaly: "RETRY_STORM_AMPLIFICATION (consecutive 503 errors)",
    errorRatePct: 3.0,
    burnRate1h: 30.0,
    sloStatus: "PAGE_CRITICAL",
    summary: "1h burn rate reached 30.0x (>= 14.4x threshold). AlertDispatcher emitted Slack webhook & Blameless Postmortem."
  }
};

function initHomePage() {
  const sel = document.getElementById("quickScenarioSelect");
  const out = document.getElementById("quickScenarioOut");
  if (!sel || !out) return;

  function renderQuick() {
    const s = TRACE_SCENARIOS[sel.value] || TRACE_SCENARIOS.normal;
    const isCrit = s.sloStatus !== "OK";
    out.innerHTML = `
      <div class="finding-item ${isCrit ? "high" : "pass"}">
        <div class="finding-head">
          <strong>${esc(s.name)}</strong>
          <span class="sev-pill ${isCrit ? "high" : "ok"}">${esc(s.sloStatus)} &middot; ${s.burnRate1h}x Burn</span>
        </div>
        <p style="margin:4px 0 0;font-size:0.86rem;color:var(--muted);">
          Spans: <strong>${s.spans}</strong> &middot; Critical Path: <strong>${s.totalMs} ms</strong> &middot; Bottleneck: <code>${esc(s.exclusiveBottleneck)}</code>
        </p>
      </div>
    `;
  }

  sel.addEventListener("change", renderQuick);
  renderQuick();
}

function initStudioPage() {
  const chips = document.querySelectorAll("[data-scenario]");
  const sloSelect = document.getElementById("studioSloTarget");
  const outEl = document.getElementById("studioOutput");
  const aegisOut = document.getElementById("studioAegisOut");
  if (!outEl) return;

  let activeKey = "n_plus_one";

  function renderStudio() {
    const s = TRACE_SCENARIOS[activeKey] || TRACE_SCENARIOS.n_plus_one;
    const targetPct = sloSelect ? Number(sloSelect.value) : 99.9;
    const budgetPct = 100 - targetPct;
    const computedBurn = budgetPct > 0 ? Number((s.errorRatePct / budgetPct).toFixed(1)) : 0;
    const severity = computedBurn >= 14.4 ? "PAGE_CRITICAL" : computedBurn >= 3.0 ? "TICKET_WARNING" : "OK";
    const isCrit = severity !== "OK";

    chips.forEach((c) => c.classList.toggle("active", c.getAttribute("data-scenario") === activeKey));

    outEl.innerHTML = `
      <div class="finding-item ${isCrit ? "high" : "pass"}">
        <div class="finding-head">
          <strong>${esc(s.name)}</strong>
          <span class="sev-pill ${isCrit ? "high" : "ok"}">${esc(severity)} &middot; ${computedBurn}x Burn Rate</span>
        </div>
        <p style="margin:4px 0 6px;font-size:0.86rem;color:var(--muted);">
          ${esc(s.summary)}
        </p>
        <p style="margin:0;font-size:0.82rem;color:var(--muted);">
          Critical Path: <strong>${s.totalMs} ms</strong> &middot; Anomaly: <strong>${esc(s.anomaly || "None")}</strong>
        </p>
      </div>
    `;

    if (aegisOut) {
      aegisOut.textContent = JSON.stringify(
        {
          gate_id: "G4_TRAJECTORY_HEALTH",
          rfc6962_prefix: "0x00",
          passed: !isCrit && !s.anomaly,
          slo_target_pct: targetPct,
          burn_rate_1h: computedBurn,
          slo_status: severity,
          anomaly_detected: s.anomaly,
          critical_path_ms: s.totalMs
        },
        null,
        2
      );
    }
  }

  chips.forEach((c) =>
    c.addEventListener("click", () => {
      activeKey = c.getAttribute("data-scenario");
      renderStudio();
    })
  );
  if (sloSelect) sloSelect.addEventListener("change", renderStudio);
  renderStudio();
}

function initRulesPage() {
  const searchInput = document.getElementById("rulesSearch");
  const catSelect = document.getElementById("rulesCategory");
  const cards = document.querySelectorAll("[data-rule-card]");
  const countEl = document.getElementById("rulesCount");
  if (!searchInput || !catSelect || !cards.length) return;

  function filterRules() {
    const q = searchInput.value.trim().toLowerCase();
    const cat = catSelect.value;
    let shown = 0;
    cards.forEach((card) => {
      const match = (cat === "all" || card.getAttribute("data-cat") === cat) && (!q || card.textContent.toLowerCase().includes(q));
      card.style.display = match ? "" : "none";
      if (match) shown++;
    });
    if (countEl) countEl.textContent = `${shown} of ${cards.length} specifications shown`;
  }

  searchInput.addEventListener("input", filterRules);
  catSelect.addEventListener("change", filterRules);
  filterRules();
}

document.addEventListener("DOMContentLoaded", () => {
  initHomePage();
  initStudioPage();
  initRulesPage();
});
