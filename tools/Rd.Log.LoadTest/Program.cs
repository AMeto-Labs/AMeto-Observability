using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Mvc;
using Rd.Log.LoadTest;

// ─────────────────────────────────────────────────────────────────────────────
// Builder
// ─────────────────────────────────────────────────────────────────────────────

var builder = WebApplication.CreateBuilder(args);

builder.Services.ConfigureHttpJsonOptions(o =>
{
    o.SerializerOptions.PropertyNamingPolicy        = JsonNamingPolicy.CamelCase;
    o.SerializerOptions.PropertyNameCaseInsensitive = true;
    o.SerializerOptions.DefaultIgnoreCondition      = JsonIgnoreCondition.WhenWritingNull;
});

// Defaults from appsettings; individual run requests can override every field.
var defaultConfig = builder.Configuration
    .GetSection("LoadTest")
    .Get<LoadTestConfig>() ?? new LoadTestConfig();

builder.Services.AddSingleton(defaultConfig);
builder.Services.AddTransient<LoadTestRunner>();

// Named HttpClient for the Rd.Log target server
builder.Services.AddHttpClient("rdlog", (sp, client) =>
{
    // Base address resolved at request time (config is mutable), so we leave it unset here.
});

// ── Run registry ──────────────────────────────────────────────────────────────

var runs = new ConcurrentDictionary<string, RunEntry>();

// ─────────────────────────────────────────────────────────────────────────────
// Application
// ─────────────────────────────────────────────────────────────────────────────

var app = builder.Build();

app.MapGet("/", () => Results.Redirect("/ui"));

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/config  — return current default config
// ─────────────────────────────────────────────────────────────────────────────
app.MapGet("/api/config", (LoadTestConfig cfg) => Results.Ok(cfg));

// ─────────────────────────────────────────────────────────────────────────────
// PUT /api/config  — update default config
// ─────────────────────────────────────────────────────────────────────────────
app.MapPut("/api/config", (LoadTestConfig patch, LoadTestConfig current) =>
{
    current.TargetUrl       = patch.TargetUrl;
    current.ApiKey          = patch.ApiKey;
    current.BatchSize       = Math.Max(1, patch.BatchSize);
    current.IntervalMs      = Math.Max(0, patch.IntervalMs);
    current.Concurrency     = Math.Clamp(patch.Concurrency, 1, 256);
    current.DurationSeconds = Math.Max(1, patch.DurationSeconds);
    current.Levels          = patch.Levels;
    if (patch.Templates is { Length: > 0 })
        current.Templates = patch.Templates;
    return Results.Ok(current);
});

// ─────────────────────────────────────────────────────────────────────────────
// POST /api/runs  — start a new run (optionally override config in body)
// ─────────────────────────────────────────────────────────────────────────────
app.MapPost("/api/runs", async (
    [FromBody] LoadTestConfig? body,
    [FromServices] LoadTestConfig  defaultCfg,
    [FromServices] IHttpClientFactory httpFactory,
    [FromServices] ILoggerFactory loggerFactory) =>
{
    // Merge: body overrides defaults for this run only
    var cfg = MergeConfig(defaultCfg, body);

    var runId   = Guid.NewGuid().ToString("N")[..8];
    var metrics = new RunMetrics();
    var cts     = new CancellationTokenSource();

    var runner  = new LoadTestRunner(cfg, httpFactory, loggerFactory.CreateLogger<LoadTestRunner>());
    // Pass the shared metrics object so the runner updates the same instance we store.
    var task    = runner.RunAsync(metrics, cts.Token);

    var entry = new RunEntry(runId, cfg, metrics, task, cts);
    runs[runId] = entry;

    return Results.Accepted($"/api/runs/{runId}", new { runId, status = "started" });
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/runs  — list all runs (with current snapshot)
// ─────────────────────────────────────────────────────────────────────────────
app.MapGet("/api/runs", () =>
    Results.Ok(runs.Values
        .OrderByDescending(r => r.Metrics.StartedAt)
        .Select(r => new RunSummary(r.Id, r.Config, r.Metrics.Snapshot()))));

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/runs/{id}  — live snapshot of a specific run
// ─────────────────────────────────────────────────────────────────────────────
app.MapGet("/api/runs/{id}", (string id) =>
    runs.TryGetValue(id, out var e)
        ? Results.Ok(new RunSummary(e.Id, e.Config, e.Metrics.Snapshot()))
        : Results.NotFound());

// ─────────────────────────────────────────────────────────────────────────────
// DELETE /api/runs/{id}  — cancel a running test
// ─────────────────────────────────────────────────────────────────────────────
app.MapDelete("/api/runs/{id}", (string id) =>
{
    if (!runs.TryGetValue(id, out var e)) return Results.NotFound();
    e.Cts.Cancel();
    return Results.NoContent();
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/runs/{id}/stream  — SSE live metrics stream
// ─────────────────────────────────────────────────────────────────────────────
app.MapGet("/api/runs/{id}/stream", async (string id, HttpContext ctx) =>
{
    if (!runs.TryGetValue(id, out var entry))
    {
        ctx.Response.StatusCode = 404;
        return;
    }

    ctx.Response.ContentType = "text/event-stream";
    ctx.Response.Headers.CacheControl = "no-cache";

    var ct = ctx.RequestAborted;
    while (!ct.IsCancellationRequested)
    {
        var snap = entry.Metrics.Snapshot();
        var json = JsonSerializer.Serialize(snap, ProgramHelpers.JsonOpts);
        await ctx.Response.WriteAsync($"data: {json}\n\n", ct);
        await ctx.Response.Body.FlushAsync(ct);

        if (!snap.IsRunning) break;

        try { await Task.Delay(500, ct); }
        catch (OperationCanceledException) { break; }
    }
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /ui  — simple HTML dashboard
// ─────────────────────────────────────────────────────────────────────────────
app.MapGet("/ui", () => Results.Content(ProgramHelpers.BuildUiHtml(), "text/html"));

app.Run();

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

static LoadTestConfig MergeConfig(LoadTestConfig defaults, LoadTestConfig? patch)
{
    if (patch is null) return defaults;
    return new LoadTestConfig
    {
        TargetUrl       = string.IsNullOrEmpty(patch.TargetUrl) ? defaults.TargetUrl : patch.TargetUrl,
        ApiKey          = patch.ApiKey ?? defaults.ApiKey,
        BatchSize       = patch.BatchSize    > 0 ? patch.BatchSize    : defaults.BatchSize,
        IntervalMs      = patch.IntervalMs   >= 0 ? patch.IntervalMs  : defaults.IntervalMs,
        Concurrency     = patch.Concurrency  > 0 ? patch.Concurrency  : defaults.Concurrency,
        DurationSeconds = patch.DurationSeconds > 0 ? patch.DurationSeconds : defaults.DurationSeconds,
        Levels          = patch.Levels,
        Templates       = patch.Templates is { Length: > 0 } ? patch.Templates : defaults.Templates,
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Records
// ─────────────────────────────────────────────────────────────────────────────

record RunEntry(
    string Id,
    LoadTestConfig Config,
    RunMetrics Metrics,
    Task Task,
    CancellationTokenSource Cts);

record RunSummary(string Id, LoadTestConfig Config, MetricSnapshot Metrics);

internal static class ProgramHelpers
{
    public static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy   = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public static string BuildUiHtml() => """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Rd.Log Load Test</title>
<style>
  :root { --bg:#0f1117;--card:#1a1d27;--border:#2a2d3e;--accent:#6c63ff;--green:#22c55e;--red:#ef4444;--yellow:#f59e0b;--text:#e2e8f0;--muted:#64748b }
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font:14px/1.5 'Inter',system-ui,sans-serif;padding:24px}
  h1{font-size:20px;font-weight:700;margin-bottom:20px}
  h2{font-size:14px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.05em;margin-bottom:12px}
  .grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;max-width:1100px}
  .card{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:20px}
  .card.wide{grid-column:1/-1}
  label{display:block;color:var(--muted);font-size:12px;margin-bottom:4px;margin-top:12px}
  label:first-of-type{margin-top:0}
  input,select{width:100%;background:#0f1117;border:1px solid var(--border);border-radius:6px;padding:7px 10px;color:var(--text);font-size:13px}
  input:focus,select:focus{outline:none;border-color:var(--accent)}
  .row{display:flex;gap:8px}
  .row input{width:auto;flex:1}
  button{padding:9px 20px;border-radius:7px;border:none;font-size:13px;font-weight:600;cursor:pointer;transition:.15s}
  .btn-start{background:var(--accent);color:#fff}
  .btn-start:hover{opacity:.85}
  .btn-stop{background:var(--red);color:#fff}
  .btn-stop:hover{opacity:.85}
  .stat-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-top:0}
  .stat{background:#12151e;border:1px solid var(--border);border-radius:8px;padding:14px}
  .stat-val{font-size:26px;font-weight:700;font-variant-numeric:tabular-nums}
  .stat-lbl{font-size:11px;color:var(--muted);margin-top:2px}
  .badge{display:inline-block;padding:2px 9px;border-radius:9999px;font-size:11px;font-weight:600}
  .badge-running{background:#16a34a22;color:var(--green);border:1px solid var(--green)}
  .badge-done{background:#64748b22;color:var(--muted);border:1px solid var(--muted)}
  .runs-table{width:100%;border-collapse:collapse;font-size:13px}
  .runs-table th{text-align:left;color:var(--muted);font-weight:500;padding:6px 10px;border-bottom:1px solid var(--border)}
  .runs-table td{padding:8px 10px;border-bottom:1px solid #1e2130}
  .runs-table tr:last-child td{border-bottom:none}
  .eps-bar{height:8px;border-radius:4px;background:var(--accent);transition:width .4s}
  .eps-track{height:8px;border-radius:4px;background:var(--border);width:100%;margin-top:6px}
  #liveChart{width:100%;height:120px}
  canvas{border-radius:6px}
</style>
</head>
<body>
<h1>⚡ Rd.Log Load Test</h1>

<div class="grid">

  <!-- Config card -->
  <div class="card">
    <h2>Configuration</h2>
    <label>Target URL</label>
    <input id="cfgTarget" value="http://localhost:5341">
    <label>API Key (optional)</label>
    <input id="cfgApiKey" placeholder="leave blank if none">
    <label>Batch size</label>
    <input id="cfgBatch" type="number" min="1" max="10000" value="10">
    <label>Interval between batches (ms) — 0 = max speed</label>
    <input id="cfgInterval" type="number" min="0" max="60000" value="100">
    <label>Workers (concurrent senders)</label>
    <input id="cfgWorkers" type="number" min="1" max="256" value="1">
    <label>Duration (seconds)</label>
    <input id="cfgDuration" type="number" min="1" max="3600" value="10">
    <div style="margin-top:16px;display:flex;gap:8px;align-items:center">
      <button class="btn-start" onclick="startRun()">▶ Start</button>
      <button class="btn-stop" onclick="stopLast()" id="btnStop" style="display:none">■ Stop</button>
      <span id="runStatus" style="color:var(--muted);font-size:12px"></span>
    </div>
  </div>

  <!-- Live metrics card -->
  <div class="card">
    <h2>Live Metrics</h2>
    <div class="stat-grid">
      <div class="stat"><div class="stat-val" id="mEvents">—</div><div class="stat-lbl">Events sent</div></div>
      <div class="stat"><div class="stat-val" id="mEps">—</div><div class="stat-lbl">Events/sec</div></div>
      <div class="stat"><div class="stat-val" id="mLatAvg">—</div><div class="stat-lbl">Avg latency ms</div></div>
      <div class="stat"><div class="stat-val" id="mErrors">—</div><div class="stat-lbl">HTTP errors</div></div>
    </div>
    <div style="margin-top:14px">
      <div style="display:flex;justify-content:space-between;font-size:11px;color:var(--muted)">
        <span id="mEpsLabel">0 eps</span>
        <span id="mEpsTarget"></span>
      </div>
      <div class="eps-track"><div class="eps-bar" id="epsBar" style="width:0%"></div></div>
    </div>
    <canvas id="liveChart" style="margin-top:14px"></canvas>
  </div>

  <!-- Throughput presets -->
  <div class="card wide">
    <h2>Throughput Presets</h2>
    <div style="display:flex;gap:10px;flex-wrap:wrap">
      <button onclick="applyPreset(1,10,1000,'100 eps')"   style="padding:7px 16px;border-radius:6px;border:1px solid var(--border);background:var(--card);color:var(--text);cursor:pointer">100 eps (1w · 10b · 1s)</button>
      <button onclick="applyPreset(1,10,100,'1k eps')"     style="padding:7px 16px;border-radius:6px;border:1px solid var(--border);background:var(--card);color:var(--text);cursor:pointer">~1k eps (1w · 10b · 100ms)</button>
      <button onclick="applyPreset(1,100,100,'1k eps')"    style="padding:7px 16px;border-radius:6px;border:1px solid var(--border);background:var(--card);color:var(--text);cursor:pointer">~1k eps (1w · 100b · 100ms)</button>
      <button onclick="applyPreset(4,100,40,'10k eps')"    style="padding:7px 16px;border-radius:6px;border:1px solid var(--border);background:var(--card);color:var(--text);cursor:pointer">~10k eps (4w · 100b · 40ms)</button>
      <button onclick="applyPreset(10,200,20,'100k eps')"  style="padding:7px 16px;border-radius:6px;border:1px solid var(--border);background:var(--card);color:var(--text);cursor:pointer">~100k eps (10w · 200b · 20ms)</button>
      <button onclick="applyPreset(20,500,0,'max')"        style="padding:7px 16px;border-radius:6px;border:1px solid var(--border);background:var(--card);color:var(--text);cursor:pointer">Max speed (20w · 500b · 0ms)</button>
    </div>
  </div>

  <!-- Run history -->
  <div class="card wide">
    <h2>Run History</h2>
    <table class="runs-table">
      <thead><tr>
        <th>ID</th><th>Target</th><th>Batch×Workers</th><th>Events</th>
        <th>eps avg</th><th>Latency avg</th><th>Errors</th><th>Duration</th><th>Status</th>
      </tr></thead>
      <tbody id="runsBody"></tbody>
    </table>
  </div>
</div>

<script>
let activeRunId = null;
let sse = null;
const epsHistory = [];

function applyPreset(workers, batch, interval, label) {
  document.getElementById('cfgWorkers').value  = workers;
  document.getElementById('cfgBatch').value    = batch;
  document.getElementById('cfgInterval').value = interval;
  setStatus('Preset: ' + label);
}

function setStatus(msg) {
  document.getElementById('runStatus').textContent = msg;
}

async function startRun() {
  const cfg = {
    targetUrl:       document.getElementById('cfgTarget').value,
    apiKey:          document.getElementById('cfgApiKey').value || null,
    batchSize:       parseInt(document.getElementById('cfgBatch').value),
    intervalMs:      parseInt(document.getElementById('cfgInterval').value),
    concurrency:     parseInt(document.getElementById('cfgWorkers').value),
    durationSeconds: parseInt(document.getElementById('cfgDuration').value),
  };

  epsHistory.length = 0;
  resetMetrics();

  const r = await fetch('/api/runs', {
    method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(cfg)
  });
  const data = await r.json();
  activeRunId = data.runId;

  setStatus('Run ' + activeRunId + ' started');
  document.getElementById('btnStop').style.display = '';

  subscribeSSE(activeRunId);
  setTimeout(refreshHistory, 500);
}

function stopLast() {
  if (!activeRunId) return;
  fetch('/api/runs/' + activeRunId, { method: 'DELETE' });
  setStatus('Stopping ' + activeRunId + '…');
}

function subscribeSSE(id) {
  if (sse) sse.close();
  sse = new EventSource('/api/runs/' + id + '/stream');
  const targetEps = calcTargetEps();
  document.getElementById('mEpsTarget').textContent = targetEps ? ('target ~' + targetEps.toLocaleString() + ' eps') : '';

  sse.onmessage = e => {
    const s = JSON.parse(e.data);
    updateMetrics(s);
    epsHistory.push(s.eventsPerSecond || 0);
    if (epsHistory.length > 60) epsHistory.shift();
    drawChart();

    if (!s.isRunning) {
      sse.close();
      document.getElementById('btnStop').style.display = 'none';
      setStatus('Run ' + id + ' finished — ' + Math.round(s.eventsPerSecond).toLocaleString() + ' eps');
      refreshHistory();
    }
  };
  sse.onerror = () => sse.close();
}

function calcTargetEps() {
  const b = parseInt(document.getElementById('cfgBatch').value) || 10;
  const w = parseInt(document.getElementById('cfgWorkers').value) || 1;
  const i = parseInt(document.getElementById('cfgInterval').value);
  if (i === 0) return null; // unknown
  return Math.round(b * w * 1000 / Math.max(1, i));
}

function updateMetrics(s) {
  document.getElementById('mEvents').textContent  = (s.eventsSent || 0).toLocaleString();
  document.getElementById('mEps').textContent     = Math.round(s.eventsPerSecond || 0).toLocaleString();
  document.getElementById('mLatAvg').textContent  = (s.avgLatencyMs || 0) + ' ms';
  document.getElementById('mErrors').textContent  = s.httpErrors || 0;
  document.getElementById('mEpsLabel').textContent = Math.round(s.eventsPerSecond || 0).toLocaleString() + ' eps';

  const target = calcTargetEps();
  const pct = target ? Math.min(100, (s.eventsPerSecond / target) * 100) : 50;
  document.getElementById('epsBar').style.width = pct + '%';
}

function resetMetrics() {
  ['mEvents','mEps','mLatAvg','mErrors'].forEach(id => document.getElementById(id).textContent = '—');
  document.getElementById('epsBar').style.width = '0%';
}

async function refreshHistory() {
  const r = await fetch('/api/runs');
  const runs = await r.json();
  const tbody = document.getElementById('runsBody');
  tbody.innerHTML = runs.map(run => {
    const m = run.metrics;
    const badge = m.isRunning
      ? '<span class="badge badge-running">running</span>'
      : '<span class="badge badge-done">done</span>';
    return `<tr>
      <td><code>${run.id}</code></td>
      <td>${run.config.targetUrl}</td>
      <td>${run.config.batchSize} × ${run.config.concurrency}w</td>
      <td>${(m.eventsSent||0).toLocaleString()}</td>
      <td>${Math.round(m.eventsPerSecond||0).toLocaleString()}</td>
      <td>${m.avgLatencyMs||0} ms</td>
      <td>${m.httpErrors||0}</td>
      <td>${Math.round(m.elapsedSeconds||0)}s</td>
      <td>${badge}</td>
    </tr>`;
  }).join('');
}

// Canvas sparkline
function drawChart() {
  const canvas = document.getElementById('liveChart');
  if (!canvas.getContext) return;
  const ctx = canvas.getContext('2d');
  canvas.width  = canvas.offsetWidth;
  canvas.height = 120;
  const w = canvas.width, h = canvas.height;
  const max = Math.max(...epsHistory, 1);
  ctx.clearRect(0, 0, w, h);

  // Grid lines
  ctx.strokeStyle = '#1e2130'; ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const y = h - (i / 4) * h;
    ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke();
  }

  if (epsHistory.length < 2) return;

  // Fill area
  const grad = ctx.createLinearGradient(0, 0, 0, h);
  grad.addColorStop(0, 'rgba(108,99,255,.35)');
  grad.addColorStop(1, 'rgba(108,99,255,0)');
  ctx.fillStyle = grad;
  ctx.beginPath();
  epsHistory.forEach((v, i) => {
    const x = (i / (epsHistory.length - 1)) * w;
    const y = h - (v / max) * h * .9;
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.lineTo(w, h); ctx.lineTo(0, h); ctx.closePath(); ctx.fill();

  // Line
  ctx.strokeStyle = '#6c63ff'; ctx.lineWidth = 2;
  ctx.beginPath();
  epsHistory.forEach((v, i) => {
    const x = (i / (epsHistory.length - 1)) * w;
    const y = h - (v / max) * h * .9;
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.stroke();

  // Max label
  ctx.fillStyle = '#64748b'; ctx.font = '11px system-ui';
  ctx.fillText(Math.round(max).toLocaleString() + ' eps', 4, 14);
}

refreshHistory();
</script>
</body>
</html>
""";
}
