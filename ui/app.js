const state = {
  bootstrap: null,
  pollTimer: null,
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
    starting: "Starting Scheduler",
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

function applyPhaseVisuals(status) {
  const phase = status.phase || "unknown";
  const dot = $("phase-dot");
  dot.className = "status-dot";
  dot.classList.add(`phase-${phase}`);
  setText("phase-label", formatPhase(phase));
  setText("scheduler-meta", status.scheduler_running ? `Scheduler: running (PID ${status.scheduler_pid})` : "Scheduler: stopped");
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

function renderHistory(history) {
  const list = $("history-list");
  list.innerHTML = "";

  if (!history || history.length === 0) {
    setVisible("history-empty", true);
    renderHistoryChart([]);
    return;
  }

  setVisible("history-empty", false);
  history.slice(-8).reverse().forEach((entry) => {
    const card = document.createElement("article");
    card.className = "history-item";
    card.innerHTML = `
      <div class="history-head">
        <strong>${entry.display_label}</strong>
        <span>${entry.cycle_started_at || "recently"}</span>
      </div>
      <div class="history-meta">
        <span>Processed ${entry.processed_files} / ${entry.scheduled_files} files</span>
        <span>Elapsed ${entry.elapsed}</span>
        <span>Avg ${entry.avg_throughput}</span>
        <span>Data ${entry.avg_data_rate}</span>
      </div>
      <div class="history-meta">
        <span>Clean ${entry.clean}</span>
        <span>Infected ${entry.infected}</span>
        <span>Vanished ${entry.vanished}</span>
        <span>Errors ${entry.errors}</span>
      </div>
    `;
    list.appendChild(card);
  });

  renderHistoryChart(history.slice(-12));
}

function renderHistoryChart(history) {
  const canvas = $("history-chart");
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, canvas.width, canvas.height);

  if (!history || history.length === 0) {
    return;
  }

  const width = canvas.width;
  const height = canvas.height;
  const padding = 26;
  const barGap = 18;
  const chartWidth = width - padding * 2;
  const chartHeight = height - padding * 2;
  const maxProcessed = Math.max(...history.map((entry) => entry.processed_files || 0), 1);
  const barWidth = Math.max(18, (chartWidth - barGap * (history.length - 1)) / history.length);

  ctx.fillStyle = "rgba(44, 32, 22, 0.08)";
  ctx.fillRect(padding, padding, chartWidth, chartHeight);

  history.forEach((entry, index) => {
    const x = padding + index * (barWidth + barGap);
    const heightScale = (entry.processed_files || 0) / maxProcessed;
    const barHeight = Math.max(8, chartHeight * heightScale);
    const y = padding + chartHeight - barHeight;
    const gradient = ctx.createLinearGradient(0, y, 0, y + barHeight);
    gradient.addColorStop(0, "#0f766e");
    gradient.addColorStop(1, "#b45309");
    ctx.fillStyle = gradient;
    ctx.fillRect(x, y, barWidth, barHeight);

    ctx.fillStyle = "#2c2016";
    ctx.font = "12px 'Trebuchet MS', sans-serif";
    ctx.fillText(entry.label === "FULL" ? "F" : "C", x + barWidth / 2 - 4, height - 8);
  });
}

function renderLogs(logs) {
  $("recent-log-output").textContent = logs && logs.length ? logs.slice(-160).join("\n") : "No log lines yet.";
}

function updateStatusView(payload) {
  const status = payload.status || payload;
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
    statusLine.textContent = "Configuration saved. Scheduler restarted with UI-managed settings.";
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

async function handleForceFull() {
  try {
    await apiFetch("/api/actions/force-full", { method: "POST", body: "{}" });
    await refreshStatus();
  } catch (error) {
    setText("last-event-value", error.message);
  }
}

async function handleRestart() {
  try {
    await apiFetch("/api/actions/restart", { method: "POST", body: "{}" });
    await refreshStatus();
  } catch (error) {
    setText("last-event-value", error.message);
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
  $("force-full-button").addEventListener("click", handleForceFull);
  $("restart-button").addEventListener("click", handleRestart);
  $("toggle-settings-button").addEventListener("click", openSettings);
  $("close-settings-button").addEventListener("click", closeSettings);
}

async function start() {
  bindEvents();
  try {
    await bootstrap();
  } catch (error) {
    setText("last-event-value", error.message);
    openSettings();
  }

  state.pollTimer = window.setInterval(refreshStatus, 5000);
}

start();
