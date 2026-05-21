using System.Buffers;
using System.Diagnostics;
using System.Net.Http.Json;
using System.Text.Json;
using MessagePack;
using Rd.Log.Core;
using Rd.Log.Core.Serialization;

namespace Rd.Log.LoadTest;

/// <summary>
/// Drives a load-test run.
///
/// Design:
///   - Spawns <see cref="LoadTestConfig.Concurrency"/> long-running worker tasks.
///   - Each worker loops: build batch → POST → record metrics → sleep(IntervalMs).
///   - The run stops after <see cref="LoadTestConfig.DurationSeconds"/> or on
///     cancellation via <see cref="CancellationToken"/>.
///
/// Throughput ceiling:
///   Theoretical max = (BatchSize × Concurrency × 1000) / max(1, IntervalMs) events/sec.
///   With Concurrency=10, BatchSize=1000, IntervalMs=0 → depends solely on server capacity.
/// </summary>
public sealed class LoadTestRunner(
    LoadTestConfig config,
    IHttpClientFactory httpFactory,
    ILogger<LoadTestRunner> logger)
{
    // ── Shared state for level-weighted random selection ──────────────────────

    private readonly int[] _levelCdf = BuildLevelCdf(config.Levels);

    // ── Public API ────────────────────────────────────────────────────────────

    /// <summary>Runs the load test, updating <paramref name="metrics"/> in real time.</summary>
    public async Task<MetricSnapshot> RunAsync(RunMetrics metrics, CancellationToken ct = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(config.DurationSeconds));

        logger.LogInformation(
            "Load test starting — target={Target} batchSize={Batch} interval={Interval}ms " +
            "workers={Workers} duration={Duration}s",
            config.TargetUrl, config.BatchSize, config.IntervalMs,
            config.Concurrency, config.DurationSeconds);

        var workers = Enumerable
            .Range(0, config.Concurrency)
            .Select(id => WorkerAsync(id, metrics, cts.Token))
            .ToArray();

        await Task.WhenAll(workers);
        metrics.MarkStopped();

        var snap = metrics.Snapshot();
        logger.LogInformation(
            "Load test finished — sent={Sent} eps={EPS:F0} dropped={Dropped} errors={Errors} " +
            "latency min/avg/max={Min}/{Avg}/{Max}ms",
            snap.EventsSent, snap.EventsPerSecond, snap.EventsDropped, snap.HttpErrors,
            snap.MinLatencyMs, snap.AvgLatencyMs, snap.MaxLatencyMs);

        return snap;
    }

    // ── Worker loop ───────────────────────────────────────────────────────────

    private async Task WorkerAsync(int workerId, RunMetrics metrics, CancellationToken ct)
    {
        using var http   = httpFactory.CreateClient("rdlog");
        http.BaseAddress = new Uri(config.TargetUrl.TrimEnd('/') + "/");
        if (!string.IsNullOrEmpty(config.ApiKey))
            http.DefaultRequestHeaders.TryAddWithoutValidation("X-Api-Key", config.ApiKey);

        var rng = new Random(workerId * 31 + Environment.TickCount);
        var sw  = new Stopwatch();

        while (!ct.IsCancellationRequested)
        {
            byte[] body;
            try
            {
                body = BuildBatch(rng, config.BatchSize);
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "Worker {Id}: batch build failed", workerId);
                continue;
            }

            sw.Restart();
            try
            {
                using var content = new ByteArrayContent(body);
                content.Headers.ContentType =
                    new System.Net.Http.Headers.MediaTypeHeaderValue("application/octet-stream");

                using var resp = await http.PostAsync("/api/events", content, ct);
                sw.Stop();

                if (resp.IsSuccessStatusCode)
                {
                    var json     = await resp.Content.ReadFromJsonAsync<JsonElement>(ct);
                    int ingested = json.TryGetProperty("ingested", out var i) ? i.GetInt32() : config.BatchSize;
                    int dropped  = json.TryGetProperty("dropped",  out var d) ? d.GetInt32() : 0;
                    metrics.RecordBatch(ingested, dropped, sw.ElapsedMilliseconds);
                }
                else
                {
                    metrics.RecordHttpError();
                    logger.LogWarning("Worker {Id}: HTTP {Status}", workerId, resp.StatusCode);
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                sw.Stop();
                metrics.RecordHttpError();
                logger.LogDebug(ex, "Worker {Id}: request failed", workerId);
            }

            if (config.IntervalMs > 0)
            {
                try { await Task.Delay(config.IntervalMs, ct); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    // ── Batch serialisation ───────────────────────────────────────────────────

    private byte[] BuildBatch(Random rng, int count)
    {
        // Serialise N events as a msgpack array of CLEF maps
        var parts = new byte[count][];
        for (int i = 0; i < count; i++)
        {
            string template = config.Templates[rng.Next(config.Templates.Length)];
            var level  = PickLevel(rng);
            var props  = BuildProps(rng, template);

            var ev = new LogEvent
            {
                Id              = new Rd.Log.Core.EventId(0, 0),
                Timestamp       = DateTimeOffset.UtcNow,
                Level           = level,
                MessageTemplate = template,
                Properties      = props,
            };
            parts[i] = LogEventSerializer.Serialize(ev);
        }

        return PackArray(parts);
    }

    private static byte[] PackArray(byte[][] parts)
    {
        int n = parts.Length;

        // msgpack array header: fixarray (≤15) or array16
        byte[] header = n <= 15
            ? [(byte)(0x90 | n)]
            : [0xdc, (byte)(n >> 8), (byte)(n & 0xff)];

        int total = header.Length + parts.Sum(p => p.Length);
        var buf   = new byte[total];
        header.CopyTo(buf, 0);
        int pos = header.Length;
        foreach (var p in parts) { p.CopyTo(buf, pos); pos += p.Length; }
        return buf;
    }

    // ── Property generation ───────────────────────────────────────────────────

    private static Dictionary<string, object?> BuildProps(Random rng, string template)
    {
        var props = new Dictionary<string, object?>();
        int start = 0;
        while (true)
        {
            int open = template.IndexOf('{', start);
            if (open < 0) break;
            int close = template.IndexOf('}', open + 1);
            if (close < 0) break;

            string name = template[(open + 1)..close];
            props[name] = GenerateValue(rng, name);
            start = close + 1;
        }
        return props;
    }

    private static object GenerateValue(Random rng, string name)
    {
        return name.ToLowerInvariant() switch
        {
            "userid"     => (long)rng.Next(1, 10_001),
            "statuscode" => (long)(rng.NextDouble() < 0.9 ? 200 : rng.Next(400, 504)),
            "durationms" => (long)rng.Next(1, 5_001),
            "attempt"    => (long)rng.Next(1, 6),
            "action"     => _actions[rng.Next(_actions.Length)],
            "method"     => _methods[rng.Next(_methods.Length)],
            "path"       => _paths[rng.Next(_paths.Length)],
            "queryname"  => _queries[rng.Next(_queries.Length)],
            "operation"  => _operations[rng.Next(_operations.Length)],
            "key"        => $"cache:{rng.Next(1, 1001)}",
            "value"      => rng.Next(0, 1000).ToString(),
            "filename"   => $"upload_{rng.Next(1, 9999)}.bin",
            "jobname"    => _jobs[rng.Next(_jobs.Length)],
            "servicename"=> _services[rng.Next(_services.Length)],
            "result"     => rng.NextDouble() < 0.95 ? "healthy" : "degraded",
            "paymentid"  => $"pay_{rng.Next(100_000, 999_999)}",
            "state"      => _paymentStates[rng.Next(_paymentStates.Length)],
            _            => (object)rng.Next(1, 9999),
        };
    }

    private static readonly string[] _actions        = ["login", "logout", "view", "create", "delete", "update", "export"];
    private static readonly string[] _methods        = ["GET", "POST", "PUT", "DELETE", "PATCH"];
    private static readonly string[] _paths          = ["/api/users", "/api/orders", "/api/products", "/api/payments", "/api/config", "/api/reports"];
    private static readonly string[] _queries        = ["GetUser", "ListOrders", "FindProduct", "CountEvents", "AggregateStats"];
    private static readonly string[] _operations     = ["read", "write", "invalidate", "refresh", "delete"];
    private static readonly string[] _jobs           = ["CleanupJob", "ReportGenerator", "DataSync", "NotificationSender", "AuditExport"];
    private static readonly string[] _services       = ["auth", "payment", "inventory", "email", "storage", "search"];
    private static readonly string[] _paymentStates  = ["pending", "authorized", "captured", "refunded", "failed"];

    // ── Level selection (weighted) ────────────────────────────────────────────

    private Rd.Log.Core.LogLevel PickLevel(Random rng)
    {
        int roll = rng.Next(0, 100);
        if (roll < _levelCdf[0]) return Rd.Log.Core.LogLevel.Verbose;
        if (roll < _levelCdf[1]) return Rd.Log.Core.LogLevel.Debug;
        if (roll < _levelCdf[2]) return Rd.Log.Core.LogLevel.Information;
        if (roll < _levelCdf[3]) return Rd.Log.Core.LogLevel.Warning;
        if (roll < _levelCdf[4]) return Rd.Log.Core.LogLevel.Error;
        return Rd.Log.Core.LogLevel.Fatal;
    }

    private static int[] BuildLevelCdf(LevelWeights w)
    {
        var weights = new[] { w.Verbose, w.Debug, w.Information, w.Warning, w.Error, w.Fatal };
        int total   = weights.Sum();
        if (total == 0) total = 1; // avoid div-by-zero
        var cdf = new int[weights.Length];
        int acc = 0;
        for (int i = 0; i < weights.Length; i++)
        {
            acc    += (int)Math.Round(weights[i] * 100.0 / total);
            cdf[i]  = acc;
        }
        return cdf;
    }
}
