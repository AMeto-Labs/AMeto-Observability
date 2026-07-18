// AMeto log-load generator.
//
// Emits realistic structured CLEF events over the server's NATIVE wire format —
// a MessagePack array of CLEF maps POSTed to /api/events (the same binary path
// Serilog.Sinks.Ameto uses), no JSON anywhere. Prints a full statistics summary
// at the end of the run.
//
//   dotnet run --project tools/loggen -- --key <api-key> [--url http://localhost:5341]
//              [--rate 10000] [--duration 60] [--batch 1000] [--concurrency 4] [--seed 42]

using System.Buffers;
using System.Diagnostics;
using System.Text.Json;
using Ameto.Core;
using MessagePack;

// ── CLI ──────────────────────────────────────────────────────────────────────
string? Arg(string name)
{
    for (int i = 0; i < args.Length - 1; i++)
        if (args[i] == name) return args[i + 1];
    return null;
}

if (args.Contains("--help") || args.Contains("-h"))
{
    Console.WriteLine("""
        loggen — AMeto log-load generator (native CLEF/MessagePack wire format)

        options:
          --key <key>          ingest API key with Logs permission (or env AMETO_API_KEY)  [required]
          --url <url>          server base URL                          default http://localhost:5341
          --rate <n>           offered events/second                    default 10000
          --duration <sec>     run length in seconds                    default 60
          --batch <n>          events per request                       default 1000
          --concurrency <n>    max in-flight requests                   default 4
          --seed <n>           RNG seed for reproducible runs           default random
        """);
    return 0;
}

string  baseUrl     = Arg("--url") ?? Environment.GetEnvironmentVariable("AMETO_URL") ?? "http://localhost:5341";
string? apiKey      = Arg("--key") ?? Environment.GetEnvironmentVariable("AMETO_API_KEY");
int     rate        = int.Parse(Arg("--rate")        ?? "10000");
double  durationSec = double.Parse(Arg("--duration") ?? "60");
int     batchSize   = int.Parse(Arg("--batch")       ?? "1000");
int     concurrency = int.Parse(Arg("--concurrency") ?? "4");
int     seed        = int.Parse(Arg("--seed")        ?? Environment.TickCount.ToString());

if (string.IsNullOrWhiteSpace(apiKey))
{
    Console.Error.WriteLine("error: --key <api-key> (or AMETO_API_KEY) is required — create one under Settings → API Keys.");
    return 1;
}

var gen = new EventGenerator(new Random(seed));
using var http = new HttpClient { BaseAddress = new Uri(baseUrl), Timeout = TimeSpan.FromSeconds(30) };
http.DefaultRequestHeaders.Add("X-Seq-ApiKey", apiKey);

int totalBatches  = Math.Max(1, (int)(rate * durationSec) / batchSize);
var batchInterval = TimeSpan.FromSeconds((double)batchSize / rate);

Console.WriteLine($"loggen → {baseUrl}/api/events  (msgpack CLEF)");
Console.WriteLine($"offered {rate:N0} ev/s × {durationSec:0.#} s · batch {batchSize} · concurrency {concurrency} · seed {seed}");
Console.WriteLine();

// ── Run ──────────────────────────────────────────────────────────────────────
var stats     = new Stats(totalBatches);
var gate      = new SemaphoreSlim(concurrency);
var inFlight  = new List<Task>(totalBatches);
var sw        = Stopwatch.StartNew();
var buffer    = new ArrayBufferWriter<byte>(batchSize * 256);
long lastProgressSent = 0;
var  lastProgressAt   = TimeSpan.Zero;

for (int b = 0; b < totalBatches; b++)
{
    // Constant-arrival pacing: batch b is due at start + b * interval.
    var due = batchInterval * b;
    var ahead = due - sw.Elapsed;
    if (ahead > TimeSpan.FromMilliseconds(1))
        await Task.Delay(ahead);

    buffer.Clear();
    gen.WriteBatch(buffer, batchSize, stats);
    byte[] payload = buffer.WrittenSpan.ToArray();   // sender owns its own copy

    await gate.WaitAsync();
    int batchIndex = b;
    inFlight.Add(Task.Run(() => SendAsync(payload, batchIndex)));

    // Progress line every ~5 s.
    if (sw.Elapsed - lastProgressAt >= TimeSpan.FromSeconds(5))
    {
        long sent = Interlocked.Read(ref stats.EventsSent);
        double evPerSec = (sent - lastProgressSent) / (sw.Elapsed - lastProgressAt).TotalSeconds;
        Console.WriteLine($"  t+{sw.Elapsed.TotalSeconds,5:0.0}s  sent {sent:N0}  ({evPerSec:N0} ev/s)  ingested {Interlocked.Read(ref stats.Ingested):N0}  dropped {Interlocked.Read(ref stats.Dropped):N0}");
        lastProgressSent = sent;
        lastProgressAt   = sw.Elapsed;
    }
}

await Task.WhenAll(inFlight);
sw.Stop();

stats.Print(sw.Elapsed, rate, batchSize, concurrency, gen);
return stats.HttpErrors > 0 ? 2 : 0;

async Task SendAsync(byte[] payload, int batchIndex)
{
    var reqSw = Stopwatch.StartNew();
    try
    {
        using var content = new ByteArrayContent(payload);
        content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/x-msgpack");
        using var res = await http.PostAsync("/api/events", content);
        reqSw.Stop();
        stats.LatenciesMs[batchIndex] = reqSw.Elapsed.TotalMilliseconds;

        if (res.IsSuccessStatusCode)
        {
            // {"ingested":N,"dropped":M} — the server's own per-batch accounting.
            using var doc = JsonDocument.Parse(await res.Content.ReadAsByteArrayAsync());
            Interlocked.Add(ref stats.Ingested, doc.RootElement.GetProperty("ingested").GetInt64());
            Interlocked.Add(ref stats.Dropped,  doc.RootElement.GetProperty("dropped").GetInt64());
            Interlocked.Add(ref stats.BytesSent, payload.Length);
        }
        else
        {
            Interlocked.Increment(ref stats.HttpErrors);
            if (Interlocked.Increment(ref stats.HttpErrorsLogged) <= 3)
                Console.Error.WriteLine($"  HTTP {(int)res.StatusCode} {res.ReasonPhrase}");
        }
    }
    catch (Exception ex)
    {
        Interlocked.Increment(ref stats.HttpErrors);
        if (Interlocked.Increment(ref stats.HttpErrorsLogged) <= 3)
            Console.Error.WriteLine($"  send failed: {ex.Message}");
    }
    finally
    {
        gate.Release();
    }
}

// ── Event generation ─────────────────────────────────────────────────────────

/// <summary>
/// Writes weighted, realistic CLEF events straight into a MessagePack buffer:
/// message templates with matching properties, per-level distribution, trace
/// correlation on a share of events, structured exceptions on errors, and an
/// occasional nested-map property.
/// </summary>
sealed class EventGenerator(Random rng)
{
    public static readonly string[] Services =
        ["Wallet.API", "Processing.API", "Etisalat.API", "dealer.Gateway", "Notification.Worker"];

    static readonly string[] Methods   = ["GET", "POST", "PUT", "DELETE"];
    static readonly string[] Paths     = ["/api/pay", "/api/topup", "/api/status", "/dealer/api/wallet", "/api/orders", "/api/balance"];
    static readonly string[] Queues    = ["payments", "notifications", "reconciliation"];
    static readonly string[] Providers = ["Visa", "Mastercard", "UnionPay"];
    static readonly string[] Auth      = ["local", "google", "microsoft"];

    static readonly ExceptionInfo GatewayTimeout = new()
    {
        Type    = "System.TimeoutException",
        Message = "The payment gateway did not respond within 30000 ms.",
        StackTrace = "   at Wallet.Gateway.PaymentClient.SendAsync(PaymentRequest request)\n   at Wallet.Api.PaymentsController.Process(PaymentDto dto)",
        Inner   = new ExceptionInfo
        {
            Type    = "System.Net.Sockets.SocketException",
            Message = "Connection timed out (10060)",
        },
    };

    static readonly ExceptionInfo InvalidState = new()
    {
        Type    = "System.InvalidOperationException",
        Message = "Sequence contains no matching element.",
        StackTrace = "   at System.Linq.ThrowHelper.ThrowNoMatchException()\n   at Processing.Api.Orders.OrderService.Complete(Int64 orderId)",
    };

    // Scenario weights out of 100 — they define the level mix printed in the summary:
    // 0..4 Info (58 %), 5..6 Debug (22 %), 7 Verbose (4 %), 8..9 Warning (10 %),
    // 10..11 Error (5 %), 12 Fatal (1 %).
    static readonly int[] Weights = [22, 12, 8, 8, 8, 14, 8, 4, 7, 3, 4, 1, 1];
    readonly int[] _cumulative = BuildCumulative();

    static int[] BuildCumulative()
    {
        var c = new int[100];
        int scenario = 0, used = 0;
        for (int i = 0; i < 100; i++)
        {
            while (scenario < Weights.Length - 1 && used >= Weights[scenario]) { scenario++; used = 0; }
            c[i] = scenario; used++;
        }
        return c;
    }

    public long ExceptionsWritten;
    public long TracedWritten;

    public void WriteBatch(IBufferWriter<byte> output, int count, Stats stats)
    {
        var w = new MessagePackWriter(output);
        w.WriteArrayHeader(count);
        for (int i = 0; i < count; i++)
            WriteEvent(ref w, stats);
        w.Flush();
    }

    void WriteEvent(ref MessagePackWriter w, Stats stats)
    {
        int scenario = _cumulative[rng.Next(100)];
        string service = Services[rng.Next(Services.Length)];

        // (level, template, userProps, exception?, traced?) per scenario.
        (LogLevel level, string template, int props, ExceptionInfo? exc, bool traced) = scenario switch
        {
            0  => (LogLevel.Information, "HTTP {Method} {Path} responded {StatusCode} in {Elapsed} ms", 6, null, rng.Next(2) == 0),
            1  => (LogLevel.Information, "Payment {PaymentId} of {Amount} {Currency} processed via {Provider}", 5, null, rng.Next(3) == 0),
            2  => (LogLevel.Information, "User {UserId} signed in from {ClientIp} via {AuthProvider}", 3, null, false),
            3  => (LogLevel.Information, "Order {OrderId} created for customer {CustomerId}: {Order}", 3, null, rng.Next(3) == 0),
            4  => (LogLevel.Information, "Wallet {WalletId} balance updated to {Balance} {Currency}", 3, null, false),
            5  => (LogLevel.Debug,       "Executed DbCommand ({Elapsed} ms) [{CommandType}] rows={Rows}", 3, null, rng.Next(3) == 0),
            6  => (LogLevel.Debug,       "Consumed message {MessageId} from {Queue} in {Elapsed} ms", 3, null, false),
            7  => (LogLevel.Verbose,     "Cache miss for key {CacheKey}", 1, null, false),
            8  => (LogLevel.Warning,     "Retry {Attempt} for request {RequestId} to {Endpoint} after {DelayMs} ms", 4, null, false),
            9  => (LogLevel.Warning,     "Slow query ({Elapsed} ms) exceeded threshold {Threshold} ms [{CommandType}]", 3, null, false),
            10 => (LogLevel.Error,       "Failed to process payment {PaymentId}: gateway timeout after {Timeout} ms", 2, GatewayTimeout, true),
            11 => (LogLevel.Error,       "Unhandled exception handling {Path} (request {RequestId})", 2, InvalidState, true),
            _  => (LogLevel.Fatal,       "Unrecoverable error in {Component}; the worker will restart", 1, GatewayTimeout, false),
        };

        int mapCount = 4 + props + (traced ? 2 : 0) + (exc is not null ? 1 : 0); // @t @mt @l service.name + rest

        w.WriteMapHeader(mapCount);
        w.Write("@t");  w.Write(DateTimeOffset.UtcNow.ToString("O"));
        w.Write("@mt"); w.Write(template);
        w.Write("@l");  w.Write(level.ToSeqString());
        w.Write("service.name"); w.Write(service);

        if (traced)
        {
            w.Write("@tr"); w.Write($"{(ulong)rng.NextInt64():x16}{(ulong)rng.NextInt64():x16}");
            w.Write("@sp"); w.Write($"{(ulong)rng.NextInt64():x16}");
            Interlocked.Increment(ref TracedWritten);
        }
        if (exc is not null)
        {
            w.Write("@x"); exc.Write(ref w);
            Interlocked.Increment(ref ExceptionsWritten);
        }

        switch (scenario)
        {
            case 0:
                w.Write("Method");     w.Write(Methods[rng.Next(Methods.Length)]);
                w.Write("Path");       w.Write(Paths[rng.Next(Paths.Length)]);
                w.Write("StatusCode"); w.Write(rng.Next(50) == 0 ? 500 : 200);
                w.Write("Elapsed");    w.Write(Math.Round(rng.NextDouble() * 240 + 1.5, 2));
                w.Write("ClientIp");   w.Write($"10.220.{rng.Next(16)}.{rng.Next(250)}");
                w.Write("RequestId");  w.Write($"req-{rng.NextInt64():x12}");
                break;
            case 1:
                w.Write("PaymentId"); w.Write($"pay_{rng.NextInt64():x12}");
                w.Write("Amount");    w.Write(Math.Round(rng.NextDouble() * 4900 + 100, 2));
                w.Write("Currency");  w.Write("AED");
                w.Write("Provider");  w.Write(Providers[rng.Next(Providers.Length)]);
                w.Write("WalletId");  w.Write(rng.Next(1_000_000, 9_999_999));
                break;
            case 2:
                w.Write("UserId");       w.Write(rng.Next(1000, 99999));
                w.Write("ClientIp");     w.Write($"10.220.{rng.Next(16)}.{rng.Next(250)}");
                w.Write("AuthProvider"); w.Write(Auth[rng.Next(Auth.Length)]);
                break;
            case 3:
                w.Write("OrderId");    w.Write(rng.Next(100_000, 999_999));
                w.Write("CustomerId"); w.Write(rng.Next(1000, 99999));
                w.Write("Order");                              // nested map property
                w.WriteMapHeader(3);
                w.Write("Id");        w.Write(rng.Next(100_000, 999_999));
                w.Write("ItemCount"); w.Write(rng.Next(1, 9));
                w.Write("Total");     w.Write(Math.Round(rng.NextDouble() * 900 + 20, 2));
                break;
            case 4:
                w.Write("WalletId"); w.Write(rng.Next(1_000_000, 9_999_999));
                w.Write("Balance");  w.Write(Math.Round(rng.NextDouble() * 90000, 2));
                w.Write("Currency"); w.Write("AED");
                break;
            case 5:
                w.Write("Elapsed");     w.Write(Math.Round(rng.NextDouble() * 45 + 0.3, 2));
                w.Write("CommandType"); w.Write(rng.Next(2) == 0 ? "SELECT" : "UPDATE");
                w.Write("Rows");        w.Write(rng.Next(0, 500));
                break;
            case 6:
                w.Write("MessageId"); w.Write($"msg-{rng.NextInt64():x12}");
                w.Write("Queue");     w.Write(Queues[rng.Next(Queues.Length)]);
                w.Write("Elapsed");   w.Write(Math.Round(rng.NextDouble() * 80 + 0.5, 2));
                break;
            case 7:
                w.Write("CacheKey"); w.Write($"wallet:{rng.Next(1_000_000, 9_999_999)}:balance");
                break;
            case 8:
                w.Write("Attempt");   w.Write(rng.Next(1, 5));
                w.Write("RequestId"); w.Write($"req-{rng.NextInt64():x12}");
                w.Write("Endpoint");  w.Write(Paths[rng.Next(Paths.Length)]);
                w.Write("DelayMs");   w.Write(250 * (1 << rng.Next(4)));
                break;
            case 9:
                w.Write("Elapsed");     w.Write(Math.Round(rng.NextDouble() * 4000 + 1000, 1));
                w.Write("Threshold");   w.Write(1000);
                w.Write("CommandType"); w.Write("SELECT");
                break;
            case 10:
                w.Write("PaymentId"); w.Write($"pay_{rng.NextInt64():x12}");
                w.Write("Timeout");   w.Write(30000);
                break;
            case 11:
                w.Write("Path");      w.Write(Paths[rng.Next(Paths.Length)]);
                w.Write("RequestId"); w.Write($"req-{rng.NextInt64():x12}");
                break;
            case 12:
                w.Write("Component"); w.Write("PaymentDispatcher");
                break;
        }

        Interlocked.Increment(ref stats.EventsSent);
        Interlocked.Increment(ref stats.LevelCounts[(int)level]);
    }
}

// ── Statistics ───────────────────────────────────────────────────────────────
sealed class Stats(int totalBatches)
{
    public long EventsSent, Ingested, Dropped, BytesSent, HttpErrors, HttpErrorsLogged;
    public readonly long[]   LevelCounts = new long[6];
    public readonly double[] LatenciesMs = new double[totalBatches];

    public void Print(TimeSpan elapsed, int rate, int batchSize, int concurrency, EventGenerator gen)
    {
        var lat = LatenciesMs.Where(static v => v > 0).OrderBy(static v => v).ToArray();
        double P(double q) => lat.Length == 0 ? 0 : lat[Math.Min(lat.Length - 1, (int)(q * lat.Length))];

        long sent    = EventsSent;
        long acked   = Ingested + Dropped;
        double secs  = elapsed.TotalSeconds;
        string lvl(LogLevel l) => sent == 0 ? "0%" : $"{100.0 * LevelCounts[(int)l] / sent:0.0}%";

        Console.WriteLine();
        Console.WriteLine("── loggen summary ─────────────────────────────────────────────");
        Console.WriteLine($"target       {rate:N0} ev/s · batch {batchSize} · concurrency {concurrency}");
        Console.WriteLine($"sent         {sent:N0} events in {LatenciesMs.Length:N0} batches · {BytesSent / 1048576.0:0.0} MB msgpack ({(sent > 0 ? (double)BytesSent / sent : 0):0} B/event)");
        Console.WriteLine($"duration     {secs:0.00} s  →  {(secs > 0 ? sent / secs : 0):N0} ev/s achieved");
        Console.WriteLine($"server       ingested {Ingested:N0} · dropped {Dropped:N0}" +
                          (acked > 0 ? $" ({100.0 * Dropped / acked:0.000} %)" : ""));
        Console.WriteLine($"http         {lat.Length:N0} ok · {HttpErrors:N0} failed");
        Console.WriteLine($"latency      p50 {P(0.50):0.0} ms · p90 {P(0.90):0.0} · p95 {P(0.95):0.0} · p99 {P(0.99):0.0} · max {(lat.Length > 0 ? lat[^1] : 0):0.0} ms");
        Console.WriteLine($"levels       V {lvl(LogLevel.Verbose)} · D {lvl(LogLevel.Debug)} · I {lvl(LogLevel.Information)} · W {lvl(LogLevel.Warning)} · E {lvl(LogLevel.Error)} · F {lvl(LogLevel.Fatal)}");
        Console.WriteLine($"extras       exceptions {gen.ExceptionsWritten:N0} · traced {gen.TracedWritten:N0} · services {EventGenerator.Services.Length}");
        Console.WriteLine("───────────────────────────────────────────────────────────────");
    }
}
