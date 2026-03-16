const state = {
  bootstrap: null,
  pollTimer: null,
  currentStatus: null,
};

const dayOptions = [
  { value: 1, label: "Mon" },
  { value: 2, label: "Tue" },
  { value: 3, label: "Wed" },
  { value: 4, label: "Thu" },
  { value: 5, label: "Fri" },
  { value: 6, label: "Sat" },
  { value: 7, label: "Sun" },
];

function $(id) {
  return document.getElementById(id);
}

async function apiFetch(path, options = {}) {
  const response = await fetch(path, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error || `Request failed for ${path}`);
  }
  return payload;
}

function setText(id, value) {
  $(id).textContent = value ?? "n/a";
}

function setVisible(id, visible) {
  $(id).classList.toggle("hidden", !visible);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function joinLines(values) {
  return (values || []).join("\n");
}

function joinCsv(values) {
  return (values || []).join(",");
}

function formatPhase(phase) {
  const labels = {
    unconfigured: "Waiting For Setup",
    config_error: "Configuration Error",
    starting: "Starting Scanner",
    idle: "Idle",
    scanning: "Scanning",
    cycle_complete: "Cycle Complete",
    waiting_lock: "Waiting For Lock",
    paused: "Paused",
    stopped: "Stopped",
  };
  return labels[phase] || phase || "Unknown";
}

function renderDayPicker(container, selectedDays) {
  container.innerHTML = "";
  const values = new Set((selectedDays || []).map(Number));
  const fieldName = container.dataset.field;

  dayOptions.forEach((day) => {
    const wrapper = document.createElement("label");
    wrapper.className = "day-chip";

    const input = document.createElement("input");
    input.type = "checkbox";
    input.name = fieldName;
    input.value = String(day.value);
    input.checked = values.has(day.value);

    const text = document.createElement("span");
    text.textContent = day.label;

    wrapper.append(input, text);
    container.appendChild(wrapper);
  });
}

function populateForm(config) {
  const form = $("config-form");
  const valueMap = {
    tz: config.tz,
    maxthreads: config.maxthreads,
    quarantine_dir: config.quarantine_dir,
    scanlog: config.scanlog,
    force_full_flag: config.force_full_flag || "",
    scan_path_marker: config.scan_path_marker || "",
    scan_paths: joinLines(config.scan_paths),
    exclude_paths: joinLines(config.exclude_paths),
    changed_scan_times: joinCsv(config.changed_scan_times),
    full_scan_times: joinCsv(config.full_scan_times),
    full_scan_parallel_jobs: config.full_scan_parallel_jobs,
    changed_scan_parallel_jobs: config.changed_scan_parallel_jobs,
    full_progress_steps: config.full_progress_steps,
    changed_progress_steps: config.changed_progress_steps,
    full_chunk_size: config.full_chunk_size,
    changed_chunk_size: config.changed_chunk_size,
    scan_failure_retry_interval: config.scan_failure_retry_interval,
    force_full_poll_interval: config.force_full_poll_interval,
    path_check_timeout: config.path_check_timeout,
    path_enumeration_timeout: config.path_enumeration_timeout,
    path_unavailable_retry_interval: config.path_unavailable_retry_interval,
  };

  Object.entries(valueMap).forEach(([name, value]) => {
    const field = form.elements.namedItem(name);
    if (field) {
      field.value = value ?? "";
    }
  });

  document.querySelectorAll(".day-picker").forEach((picker) => {
    renderDayPicker(picker, config[picker.dataset.field] || []);
  });
}

function collectCheckboxValues(fieldName) {
  return Array.from(document.querySelectorAll(`input[name="${fieldName}"]:checked`))
    .map((input) => Number(input.value))
    .sort((a, b) => a - b);
}

function splitMultiline(value) {
  return value
    .split("\n")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function splitCsv(value) {
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function collectFormPayload() {
  const form = $("config-form");
  return {
    tz: form.tz.value.trim(),
    maxthreads: Number(form.maxthreads.value),
    quarantine_dir: form.quarantine_dir.value.trim(),
    scanlog: form.scanlog.value.trim(),
    force_full_flag: form.force_full_flag.value.trim(),
    scan_path_marker: form.scan_path_marker.value.trim(),
    scan_paths: splitMultiline(form.scan_paths.value),
    exclude_paths: splitMultiline(form.exclude_paths.value),
    changed_scan_days: collectCheckboxValues("changed_scan_days"),
    changed_scan_times: splitCsv(form.changed_scan_times.value),
    full_scan_days: collectCheckboxValues("full_scan_days"),
    full_scan_times: splitCsv(form.full_scan_times.value),
    full_scan_parallel_jobs: Number(form.full_scan_parallel_jobs.value),
    changed_scan_parallel_jobs: Number(form.changed_scan_parallel_jobs.value),
    full_progress_steps: Number(form.full_progress_steps.value),
    changed_progress_steps: Number(form.changed_progress_steps.value),
    full_chunk_size: Number(form.full_chunk_size.value),
    changed_chunk_size: Number(form.changed_chunk_size.value),
    scan_failure_retry_interval: Number(form.scan_failure_retry_interval.value),
    force_full_poll_interval: Number(form.force_full_poll_interval.value),
    path_check_timeout: Number(form.path_check_timeout.value),
    path_enumeration_timeout: Number(form.path_enumeration_timeout.value),
    path_unavailable_retry_interval: Number(form.path_unavailable_retry_interval.value),
  };
}

function openSettings() {
  $("settings-drawer").classList.remove("hidden");
}

function closeSettings() {
  $("settings-drawer").classList.add("hidden");
}

function openManualFullDrawer() {
  closeManualScanDrawer();
  $("manual-full-drawer").classList.remove("hidden");
}

function closeManualFullDrawer() {
  $("manual-full-drawer").classList.add("hidden");
}

function openManualScanDrawer() {
  closeManualFullDrawer();
  $("manual-scan-drawer").classList.remove("hidden");
}

function closeManualScanDrawer() {
  $("manual-scan-drawer").classList.add("hidden");
}

function updateManualModeVisibility() {
  const selected = document.querySelector('input[name="manual_mode"]:checked')?.value || "since_last";
  setVisible("manual-lookback-wrap", selected === "relative");
}

function applyPhaseVisuals(status) {
  const phase = status.phase || "unknown";
  const dot = $("phase-dot");
  dot.className = "status-dot";
  dot.classList.add(`phase-${phase}`);
  setText("phase-label", formatPhase(phase));
  setText("scheduler-meta", status.scheduler_running ? `Scanner: running (PID ${status.scheduler_pid})` : "Scanner: stopped");
  setText("next-wake-meta", status.next_wake ? `Next wake: ${status.next_wake}` : "Next wake: pending");
  setText("flag-meta", `Force flag: ${status.effective_force_full_flag}`);
}

function updateCurrentScan(scan) {
  if (!scan) {
    setText("scan-title", "No active scan");
    setVisible("scan-kind-badge", false);
    $("progress-bar").style.width = "0%";
    setText("progress-percent", "0%");
    setText("progress-numbers", "0 / 0 files");
    setText("bytes-progress", "0 / 0");
    setText("elapsed-value", "n/a");
    setText("avg-throughput-value", "n/a");
    setText("window-throughput-value", "n/a");
    setText("avg-data-rate-value", "n/a");
    setText("window-data-rate-value", "n/a");
    setText("clean-count", "0");
    setText("infected-count", "0");
    setText("vanished-count", "0");
    setText("error-count", "0");
    setText("progress-interval-value", "n/a");
    setText("workers-value", "n/a");
    return;
  }

  setText("scan-title", scan.display_label || "Scan in progress");
  $("scan-kind-badge").textContent = scan.label || "SCAN";
  setVisible("scan-kind-badge", true);
  $("progress-bar").style.width = `${scan.percent || 0}%`;
  setText("progress-percent", `${scan.percent || 0}%`);
  setText("progress-numbers", `${scan.processed_files || 0} / ${scan.total_files || 0} files`);
  setText("bytes-progress", `${scan.processed_bytes || "0"} / ${scan.total_bytes || "0"}`);
  setText("elapsed-value", scan.elapsed || "n/a");
  setText("avg-throughput-value", scan.avg_throughput || "n/a");
  setText("window-throughput-value", scan.window_throughput || "n/a");
  setText("avg-data-rate-value", scan.avg_data_rate || "n/a");
  setText("window-data-rate-value", scan.window_data_rate || "n/a");
  setText("clean-count", String(scan.clean ?? 0));
  setText("infected-count", String(scan.infected ?? 0));
  setText("vanished-count", String(scan.vanished ?? 0));
  setText("error-count", String(scan.errors ?? 0));
  setText("progress-interval-value", scan.progress_interval ? `${scan.progress_interval} files` : "n/a");
  setText("workers-value", scan.workers ? String(scan.workers) : "n/a");
}

function updateRuntimePanel(status) {
  setText("last-event-value", status.last_event || "No events yet.");
  setText("last-warning-value", status.last_warning || "None");
  setText("pid-value", status.scheduler_running ? String(status.scheduler_pid) : "n/a");
  setText("scanlog-value", status.scanlog || "n/a");
}

function formatRateNumber(value) {
  if (!Number.isFinite(value)) {
    return "n/a";
  }
  if (value >= 100) {
    return value.toFixed(0);
  }
  if (value >= 10) {
    return value.toFixed(1);
  }
  if (value >= 1) {
    return value.toFixed(2);
  }
  if (value === 0) {
    return "0";
  }
  return value.toFixed(3);
}

function formatFilesRateValue(value) {
  return Number.isFinite(value) ? `${formatRateNumber(value)} files/s` : "n/a";
}

function formatDataRateValue(mibPerSecond) {
  if (!Number.isFinite(mibPerSecond)) {
    return "n/a";
  }

  const units = [
    { label: "TiB/s", factor: 1024 * 1024 },
    { label: "GiB/s", factor: 1024 },
    { label: "MiB/s", factor: 1 },
    { label: "KiB/s", factor: 1 / 1024 },
    { label: "B/s", factor: 1 / (1024 * 1024) },
  ];

  for (const unit of units) {
    if (mibPerSecond >= unit.factor || unit.label === "B/s") {
      return `${formatRateNumber(mibPerSecond / unit.factor)} ${unit.label}`;
    }
  }

  return "n/a";
}

function buildTraceSamples(trace, valueKey) {
  return (trace || [])
    .map((point, index) => {
      const rawValue = Number(point?.[valueKey]);
      const elapsedSeconds = Number(point?.elapsed_seconds);
      return {
        elapsedSeconds: Number.isFinite(elapsedSeconds) ? elapsedSeconds : index,
        value: Number.isFinite(rawValue) ? rawValue : null,
      };
    })
    .filter((point) => point.value !== null);
}

function buildTraceSvg(samples, color) {
  const width = 320;
  const height = 112;
  const paddingX = 10;
  const paddingY = 12;
  const plotWidth = width - paddingX * 2;
  const plotHeight = height - paddingY * 2;

  if (!samples.length) {
    return `
      <svg viewBox="0 0 ${width} ${height}" class="trace-svg" aria-hidden="true">
        <rect x="${paddingX}" y="${paddingY}" width="${plotWidth}" height="${plotHeight}" rx="12" fill="rgba(44, 32, 22, 0.04)"></rect>
      </svg>
    `;
  }

  const maxElapsed = Math.max(samples[samples.length - 1].elapsedSeconds, 1);
  const maxValue = Math.max(...samples.map((point) => point.value), 1);
  const midY = paddingY + plotHeight / 2;
  const bottomY = paddingY + plotHeight;

  const points = samples.map((point, index) => {
    const x = paddingX + (index === 0 ? 0 : (point.elapsedSeconds / maxElapsed) * plotWidth);
    const y = paddingY + (1 - point.value / maxValue) * plotHeight;
    return { x, y };
  });

  const linePath = points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`).join(" ");
  const areaPath = `${linePath} L ${points[points.length - 1].x.toFixed(2)} ${bottomY.toFixed(2)} L ${points[0].x.toFixed(2)} ${bottomY.toFixed(2)} Z`;
  const endPoint = points[points.length - 1];

  return `
    <svg viewBox="0 0 ${width} ${height}" class="trace-svg" aria-hidden="true">
      <rect x="${paddingX}" y="${paddingY}" width="${plotWidth}" height="${plotHeight}" rx="12" fill="rgba(44, 32, 22, 0.04)"></rect>
      <line x1="${paddingX}" y1="${paddingY}" x2="${width - paddingX}" y2="${paddingY}" class="trace-grid-line"></line>
      <line x1="${paddingX}" y1="${midY}" x2="${width - paddingX}" y2="${midY}" class="trace-grid-line"></line>
      <line x1="${paddingX}" y1="${bottomY}" x2="${width - paddingX}" y2="${bottomY}" class="trace-grid-line"></line>
      <path d="${areaPath}" fill="${color}" fill-opacity="0.12"></path>
      <path d="${linePath}" fill="none" stroke="${color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></path>
      <circle cx="${endPoint.x.toFixed(2)}" cy="${endPoint.y.toFixed(2)}" r="4.5" fill="${color}"></circle>
    </svg>
  `;
}

function renderHistoryTraces(history) {
  const container = $("history-traces");
  container.innerHTML = "";

  const entries = (history || [])
    .filter((entry) => Array.isArray(entry.progress_trace) && entry.progress_trace.length > 1)
    .slice(-3)
    .reverse();

  if (!entries.length) {
    const empty = document.createElement("div");
    empty.className = "trace-empty";
    empty.textContent = "Detailed speed traces will appear here after recent scans complete with progress checkpoints.";
    container.appendChild(empty);
    return;
  }

  entries.forEach((entry) => {
    const throughputSamples = buildTraceSamples(entry.progress_trace, "window_throughput_files_per_sec");
    const dataRateSamples = buildTraceSamples(entry.progress_trace, "window_data_rate_mib_per_sec");
    const latestThroughput = throughputSamples.length ? throughputSamples[throughputSamples.length - 1].value : NaN;
    const peakThroughput = throughputSamples.length ? Math.max(...throughputSamples.map((point) => point.value)) : NaN;
    const latestDataRate = dataRateSamples.length ? dataRateSamples[dataRateSamples.length - 1].value : NaN;
    const peakDataRate = dataRateSamples.length ? Math.max(...dataRateSamples.map((point) => point.value)) : NaN;

    const card = document.createElement("article");
    card.className = "trace-card";
    card.innerHTML = `
      <div class="trace-card-head">
        <div>
          <p class="trace-overline">${escapeHtml(entry.display_label || "Recent Scan")}</p>
          <h3>${escapeHtml(entry.cycle_started_at || "Recently completed")}</h3>
        </div>
        <div class="trace-badge trace-badge-${escapeHtml((entry.label || "CHANGED").toLowerCase())}">
          ${escapeHtml(entry.label || "SCAN")}
        </div>
      </div>
      <div class="trace-chart-grid">
        <section class="trace-metric-card">
          <div class="trace-metric-head">
            <div>
              <span class="trace-metric-label">Window Throughput</span>
              <strong>${escapeHtml(formatFilesRateValue(latestThroughput))}</strong>
            </div>
            <span class="trace-metric-meta">Peak ${escapeHtml(formatFilesRateValue(peakThroughput))}</span>
          </div>
          ${buildTraceSvg(throughputSamples, "#0f766e")}
        </section>
        <section class="trace-metric-card">
          <div class="trace-metric-head">
            <div>
              <span class="trace-metric-label">Window Data Rate</span>
              <strong>${escapeHtml(formatDataRateValue(latestDataRate))}</strong>
            </div>
            <span class="trace-metric-meta">Peak ${escapeHtml(formatDataRateValue(peakDataRate))}</span>
          </div>
          ${buildTraceSvg(dataRateSamples, "#b45309")}
        </section>
      </div>
      <div class="trace-footer">
        <span>${escapeHtml(`${entry.progress_trace.length} checkpoints across ${entry.elapsed || "the scan"}`)}</span>
        <span>${escapeHtml(`Final averages: ${entry.avg_throughput || "n/a"} and ${entry.avg_data_rate || "n/a"}`)}</span>
      </div>
    `;
    container.appendChild(card);
  });
}

function renderHistory(history) {
  const list = $("history-list");
  list.innerHTML = "";

  if (!history || history.length === 0) {
    setVisible("history-empty", true);
    renderHistoryTraces([]);
    return;
  }

  setVisible("history-empty", false);
  renderHistoryTraces(history);

  history.slice(-8).reverse().forEach((entry) => {
    const card = document.createElement("article");
    card.className = "history-item";
    card.innerHTML = `
      <div class="history-head">
        <strong>${escapeHtml(entry.display_label)}</strong>
        <span>${escapeHtml(entry.cycle_started_at || "recently")}</span>
      </div>
      <div class="history-meta">
        <span>${escapeHtml(`Processed ${entry.processed_files} / ${entry.scheduled_files} files`)}</span>
        <span>${escapeHtml(`Elapsed ${entry.elapsed}`)}</span>
        <span>${escapeHtml(`Avg ${entry.avg_throughput}`)}</span>
        <span>${escapeHtml(`Data ${entry.avg_data_rate}`)}</span>
      </div>
      <div class="history-meta">
        <span>${escapeHtml(`Clean ${entry.clean}`)}</span>
        <span>${escapeHtml(`Infected ${entry.infected}`)}</span>
        <span>${escapeHtml(`Vanished ${entry.vanished}`)}</span>
        <span>${escapeHtml(`Errors ${entry.errors}`)}</span>
      </div>
    `;
    list.appendChild(card);
  });
}

function renderLogs(logs) {
  $("recent-log-output").textContent = logs && logs.length ? logs.slice(-160).join("\n") : "No log lines yet.";
}

function updateStatusView(payload) {
  const status = payload.status || payload;
  state.currentStatus = status;
  applyPhaseVisuals(status);
  updateCurrentScan(status.current_scan);
  updateRuntimePanel(status);
  renderHistory(payload.history || status.history || []);
  renderLogs(payload.recent_logs || status.recent_logs || []);

  setVisible("setup-banner", !status.configured);
  if (!status.configured) {
    openSettings();
  }
}

async function refreshStatus() {
  try {
    const payload = await apiFetch("/api/status");
    updateStatusView(payload);
  } catch (error) {
    setText("last-event-value", error.message);
  }
}

async function handleConfigSubmit(event) {
  event.preventDefault();
  const statusLine = $("form-status");
  statusLine.textContent = "Saving configuration...";

  try {
    const payload = collectFormPayload();
    const response = await apiFetch("/api/config", {
      method: "PUT",
      body: JSON.stringify(payload),
    });
    statusLine.textContent = "Configuration saved. Scanner restarted with UI-managed settings.";
    updateStatusView({
      status: response.status,
      history: response.status.history,
      recent_logs: response.status.recent_logs,
    });
    closeSettings();
    await refreshStatus();
  } catch (error) {
    statusLine.textContent = error.message;
  }
}

function collectManualFullPayload() {
  const form = $("manual-full-form");
  return {
    target_paths: splitMultiline(form.manual_full_target_paths.value),
    ignore_paths: splitMultiline(form.manual_full_ignore_paths.value),
  };
}

async function handleManualFullSubmit(event) {
  event.preventDefault();
  const statusLine = $("manual-full-form-status");
  statusLine.textContent = "Queueing full scan...";

  try {
    const payload = collectManualFullPayload();
    await apiFetch("/api/actions/manual-full", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    statusLine.textContent = "Full scan queued. It will run after any current scan finishes.";
    closeManualFullDrawer();
    await refreshStatus();
  } catch (error) {
    statusLine.textContent = error.message;
  }
}

async function handleRestart() {
  try {
    if (state.currentStatus?.current_scan) {
      const confirmed = window.confirm(
        "This restarts only the scanner process inside the container, not the whole container. The current scan will stop and be retried later because scan checkpoints are only advanced after successful completion. Continue?",
      );
      if (!confirmed) {
        return;
      }
    }

    await apiFetch("/api/actions/restart-scanner", { method: "POST", body: "{}" });
    await refreshStatus();
  } catch (error) {
    setText("last-event-value", error.message);
  }
}

function collectManualScanPayload() {
  const form = $("manual-scan-form");
  const mode = document.querySelector('input[name="manual_mode"]:checked')?.value || "since_last";
  const payload = {
    mode,
    target_paths: splitMultiline(form.manual_target_paths.value),
    ignore_paths: splitMultiline(form.manual_ignore_paths.value),
  };

  if (mode === "relative") {
    const value = Number(form.manual_lookback_value.value);
    const unit = form.manual_lookback_unit.value;
    const multipliers = {
      minutes: 60,
      hours: 3600,
      days: 86400,
    };
    payload.lookback_seconds = value * (multipliers[unit] || 60);
  }

  return payload;
}

async function handleManualScanSubmit(event) {
  event.preventDefault();
  const statusLine = $("manual-form-status");
  statusLine.textContent = "Queueing changed scan...";

  try {
    const payload = collectManualScanPayload();
    await apiFetch("/api/actions/manual-changed", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    statusLine.textContent = "Changed scan queued. It will run after any current scan finishes.";
    closeManualScanDrawer();
    await refreshStatus();
  } catch (error) {
    statusLine.textContent = error.message;
  }
}

async function bootstrap() {
  const payload = await apiFetch("/api/bootstrap");
  state.bootstrap = payload;
  populateForm(payload.config || payload.defaults);
  updateStatusView(payload);
  if (payload.config_error) {
    $("form-status").textContent = payload.config_error;
  }
}

function bindEvents() {
  $("config-form").addEventListener("submit", handleConfigSubmit);
  $("manual-full-form").addEventListener("submit", handleManualFullSubmit);
  $("manual-scan-form").addEventListener("submit", handleManualScanSubmit);
  $("manual-full-button").addEventListener("click", openManualFullDrawer);
  $("manual-changed-button").addEventListener("click", openManualScanDrawer);
  $("restart-button").addEventListener("click", handleRestart);
  $("toggle-settings-button").addEventListener("click", openSettings);
  $("close-settings-button").addEventListener("click", closeSettings);
  $("close-manual-full-button").addEventListener("click", closeManualFullDrawer);
  $("close-manual-scan-button").addEventListener("click", closeManualScanDrawer);
  document.querySelectorAll('input[name="manual_mode"]').forEach((input) => {
    input.addEventListener("change", updateManualModeVisibility);
  });
}

async function start() {
  bindEvents();
  try {
    await bootstrap();
  } catch (error) {
    setText("last-event-value", error.message);
    openSettings();
  }

  updateManualModeVisibility();

  state.pollTimer = window.setInterval(refreshStatus, 5000);
}

start();
