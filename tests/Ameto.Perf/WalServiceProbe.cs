using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using MessagePack;
using Ameto.Core;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// What carrying the event's service costs the log WAL (#111): bytes per event and append time for
/// the format chosen — v5, a 16-bit pool index in every entry plus a flag bit — against the other
/// shape the issue named, a per-batch service table: a record written only when the service CHANGES
/// from one entry to the next, the entries themselves staying v4-sized.
///
/// <para>The table is not implemented; it is priced from the same event streams. Its entries are
/// v4's, and each change of service adds a record. The smallest record this log can carry is an
/// entry of its own — a header and no payload — so a change is charged 26 bytes and the cost of one
/// empty entry. Those figures are a FLOOR for the table: the real thing also needs a record type the
/// replay walks, state carried from entry to entry, and a rule for the first entry after a rotation.</para>
///
/// <para>The streams are what the drainer hands TryWrite: the ring in enqueue order. A lone sender
/// gives long runs of one service (one service per batch, the common case); an OTel collector
/// forwards several resources per export; concurrent senders interleave down to single events.</para>
///
/// <para>Time is measured twice. LAYOUT: the per-entry work the two layouts differ by — write the
/// header, CRC32C it with the payload, copy the payload — into a buffer that is already resident,
/// which is a clean comparison. END TO END: the real <see cref="WriteAheadLog.Append"/>, lock and all,
/// into a fresh mapping, where every round also pays the first-touch faults of pages the WAL has
/// not written yet, exactly as a live WAL does; on a shared box that spread is wider than the
/// difference being looked for, so the two streams compared are timed in alternating rounds.</para>
///
/// <para>The one thing asserted is the hot-path rule: an append that names a service allocates
/// nothing once its pool row is written. Timings are printed, not gated (Debug in CI, a shared box
/// here); run under -c Release for figures worth quoting.</para>
/// </summary>
public sealed class WalServiceProbe
{
    private const int  Events   = 16_000;            // per timed round
    private const int  Rounds   = 7;
    private const long Capacity = 64L * 1024 * 1024; // the production default; warm-up + rounds stay under it, so no Grow

    private readonly ITestOutputHelper _out;
    public WalServiceProbe(ITestOutputHelper o) => _out = o;

    private sealed record Pattern(string Name, Func<int, int> ServiceOf);

    private static readonly Pattern NoService = new("no service", static _ => -1);

    private static readonly Pattern[] Patterns =
    [
        new("one service",                        static _ => 0),
        new("batches of 512, 4 senders in turn",  static i => i / 512 % 4),
        new("OTel collector: 8 resources x 64",   static i => i / 64 % 8),
        new("4 senders interleaved per event",    static i => i % 4),
        new("300 services, random",               static i => (int)((uint)(i * 2654435761u) % 300)),
    ];

    [Fact]
    public void Carrying_the_service_costs_two_bytes_an_event_and_allocates_nothing()
    {
        var payloads = new (string Name, byte[] Bytes)[]
        {
            ("tiny {n}",                      Tiny()),
            ("Serilog web request",           Serilog()),
            ("OTLP record + resource attrs",  Otlp()),
        };

        _out.WriteLine($"BYTES per event, {Events:N0}-event streams: v5 pays 2 on every entry, the table 26 per change of service");
        foreach (var (name, payload) in payloads)
        {
            int v4 = 24 + payload.Length;
            _out.WriteLine($"  {name} ({payload.Length} B of properties): v4 entry {v4} B, v5 entry {v4 + 2} B (+{200.0 / v4:F2} %)");
            foreach (var p in Patterns)
            {
                int    changes = Changes(p);
                double table   = v4 + 26.0 * changes / Events;
                _out.WriteLine($"    table, {p.Name,-36} {changes,6} changes: {table,7:F2} B (+{(table - v4) * 100 / v4:F2} %)");
            }
        }

        _out.WriteLine($"\nLAYOUT time per entry, resident buffer (header + CRC32C + copy), best of {LayoutRounds} alternating rounds");
        var (_, recordNs) = LayoutNs([]);
        _out.WriteLine($"  a table record (26 B, no payload): {recordNs,6:F1} ns");
        foreach (var (name, payload) in payloads)
        {
            var (v4, v5) = LayoutNs(payload);
            _out.WriteLine($"  {name,-30} v4-sized {v4,6:F1} ns   v5 {v5,6:F1} ns   (v5 - v4 {v5 - v4,5:F1} ns; " +
                           $"the table adds {recordNs:F1} ns per change: +{recordNs:F1} ns/event interleaved)");
        }

        string dir = Path.Combine(Path.GetTempPath(), "ameto-walsvc-probe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var (emptyNs, _) = Paired(dir, [], NoService, NoService);
            _out.WriteLine($"\nEND TO END: WriteAheadLog.Append, fresh mapping, best of {Rounds} alternating rounds; " +
                           $"an empty append (the table's record): {emptyNs:F1} ns");
            foreach (var (name, payload) in payloads)
            {
                _out.WriteLine($"  {name}");
                foreach (var p in Patterns)
                {
                    var (noneNs, v5Ns) = Paired(dir, payload, NoService, p);
                    _out.WriteLine($"    {p.Name,-36} no service {noneNs,7:F1} ns   v5 {v5Ns,7:F1} ns   " +
                                   $"table (floor) {noneNs + Changes(p) * emptyNs / Events,7:F1} ns");
                }
            }

            // The rule this probe exists to pin: once a WAL has written a service's pool row, an
            // append naming that service allocates nothing — no string, no row, no boxing.
            using var wal = Session.Open(dir, Serilog(), Patterns[^1]);
            wal.Round();                                    // warm: JIT, the 300 pool rows
            long a0 = GC.GetAllocatedBytesForCurrentThread();
            wal.Round();
            Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - a0);
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { }
        }
    }

    /// <summary>How many entries of the stream name a different service than the one before.</summary>
    private static int Changes(Pattern p)
    {
        int changes = 0, prev = int.MinValue;
        for (int i = 0; i < Events; i++)
        {
            int s = p.ServiceOf(i);
            if (s != prev) changes++;
            prev = s;
        }
        return changes;
    }

    private const int LayoutRounds = 25;

    /// <summary>
    /// Best-of-<see cref="LayoutRounds"/> nanoseconds for the work an entry costs whatever surrounds
    /// it — header fields stored, CRC32C over the header (checksum excluded) and the payload, payload
    /// copied — in the v4 layout (24-byte header, the table's entry) and the v5 one (26 bytes), timed
    /// in alternating rounds into a buffer made resident before the clock starts. Append's arithmetic
    /// copied, not called: this prices the two layouts against each other, and WriteAheadLogTests pins
    /// the bytes the real one writes.
    /// </summary>
    private static unsafe (double V4Ns, double V5Ns) LayoutNs(byte[] payload)
    {
        const int Bytes = 8 * 1024 * 1024;
        byte* buf = (byte*)NativeMemory.Alloc(Bytes);
        try
        {
            new Span<byte>(buf, Bytes).Clear();
            var    sw = new Stopwatch();
            double v4 = double.MaxValue, v5 = double.MaxValue;
            for (int r = 0; r <= LayoutRounds; r++)          // round 0 warms
            {
                double a = Round(24), b = Round(26);
                if (r == 0) continue;
                v4 = Math.Min(v4, a);
                v5 = Math.Min(v5, b);
            }
            return (v4, v5);

            double Round(int headerBytes)
            {
                long pos = 0;
                sw.Restart();
                for (int i = 0; i < Events; i++)
                {
                    int size = headerBytes + payload.Length;
                    if (pos + size > Bytes) pos = 0;
                    byte* e = buf + pos;
                    Unsafe.WriteUnaligned(e,      (uint)payload.Length);
                    Unsafe.WriteUnaligned(e + 4,  (long)i);
                    e[12] = (byte)LogLevel.Information;
                    e[13] = headerBytes == 26 ? (byte)0x02 : (byte)0;
                    Unsafe.WriteUnaligned(e + 14, (ushort)3);
                    Unsafe.WriteUnaligned(e + 16, 0u);
                    if (headerBytes == 26) Unsafe.WriteUnaligned(e + 20, (ushort)(i & 7));
                    uint crc = Crc32c.Append(0, new ReadOnlySpan<byte>(e, headerBytes - 4));
                    crc      = Crc32c.Append(crc, payload);
                    Unsafe.WriteUnaligned(e + headerBytes - 4, crc);
                    payload.CopyTo(new Span<byte>(e + headerBytes, payload.Length));
                    pos += size;
                }
                sw.Stop();
                return sw.Elapsed.TotalNanoseconds / Events;
            }
        }
        finally { NativeMemory.Free(buf); }
    }

    /// <summary>
    /// Best-of-<see cref="Rounds"/> nanoseconds per <see cref="WriteAheadLog.Append"/> for two streams,
    /// each in a WAL of its own, timed in alternating rounds after a warm-up round each (JIT, every pool
    /// row the stream needs). Every round appends to pages the mapping has not touched yet, as the live
    /// WAL always does, so first-touch faults are in both figures alike.
    /// </summary>
    private static (double FirstNs, double SecondNs) Paired(string dir, byte[] payload, Pattern first, Pattern second)
    {
        using var a = Session.Open(dir, payload, first);
        using var b = Session.Open(dir, payload, second);
        a.Round();
        b.Round();

        double bestA = double.MaxValue, bestB = double.MaxValue;
        for (int r = 0; r < Rounds; r++)
        {
            bestA = Math.Min(bestA, a.Round());
            bestB = Math.Min(bestB, b.Round());
        }
        return (bestA, bestB);
    }

    /// <summary>One stream into a fresh WAL, a round at a time.</summary>
    private sealed class Session : IDisposable
    {
        private static readonly string[] Services =
            Enumerable.Range(0, 300).Select(static s => "Svc." + s + ".Api").ToArray();

        private readonly WriteAheadLog _wal;
        private readonly string        _path;
        private readonly byte[]        _payload;
        private readonly Pattern       _pattern;
        private readonly Stopwatch     _sw = new();

        private Session(string path, byte[] payload, Pattern pattern)
        {
            _path    = path;
            _payload = payload;
            _pattern = pattern;
            _wal     = WriteAheadLog.Open(path, NodeId.Local, new SegmentId(1UL), Capacity);
        }

        public static Session Open(string dir, byte[] payload, Pattern pattern) =>
            new(Path.Combine(dir, Guid.NewGuid().ToString("N") + ".wal"), payload, pattern);

        /// <summary>Appends <see cref="Events"/> entries; nanoseconds per append.</summary>
        public double Round()
        {
            _sw.Restart();
            for (int i = 0; i < Events; i++)
            {
                int s = _pattern.ServiceOf(i);
                _wal.Append(i, LogLevel.Information, 3, "{Method} {Path} responded {StatusCode} in {Elapsed} ms",
                            _payload, exception: null,
                            serviceIndex: s < 0 ? -1 : 100 + s, service: s < 0 ? null : Services[s]);
            }
            _sw.Stop();
            return _sw.Elapsed.TotalNanoseconds / Events;
        }

        public void Dispose()
        {
            _wal.Dispose();
            try { File.Delete(_path); File.Delete(_path + ".pool"); } catch { }
        }
    }

    // ── Payloads: the msgpack property map an entry carries ─────────────────────

    private static byte[] Tiny()
    {
        var buf = new ArrayBufferWriter<byte>(16);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(1);
        w.Write("n"); w.Write(42);
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>The corpus the aggregation and filter probes use, with a web app's usual enrichers.</summary>
    private static byte[] Serilog()
    {
        var buf = new ArrayBufferWriter<byte>(512);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(9);
        w.Write("SourceContext");      w.Write("Common.MediatR.LoggingBehavior");
        w.Write("ApplicationContext"); w.Write("Office.API");
        w.Write("RequestPath");        w.Write("/api/v1/orders/48213");
        w.Write("RequestId");          w.Write("0HN7M3QK4C1S2:00000004");
        w.Write("ConnectionId");       w.Write("0HN7M3QK4C1S2");
        w.Write("Elapsed");            w.Write(12.874);
        w.Write("StatusCode");         w.Write(200);
        w.Write("MachineName");        w.Write("office-api-7d9f8b6c4-xk2lp");
        w.Write("EnvironmentName");    w.Write("Production");
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }

    /// <summary>
    /// An OTLP record as the parsers write it: the resource's attributes other than
    /// service.name (which became the header) followed by the record's own, and the trace context.
    /// </summary>
    private static byte[] Otlp()
    {
        var buf = new ArrayBufferWriter<byte>(1024);
        var w   = new MessagePackWriter(buf);
        w.WriteMapHeader(14);
        w.Write("host.name");                 w.Write("aks-nodepool1-31718369-vmss000004");
        w.Write("k8s.pod.name");              w.Write("payments-gateway-6b8d9f7c5-q8w2n");
        w.Write("k8s.namespace.name");        w.Write("payments");
        w.Write("k8s.node.name");             w.Write("aks-nodepool1-31718369-vmss000004");
        w.Write("deployment.environment");    w.Write("production");
        w.Write("service.version");           w.Write("2.14.3");
        w.Write("service.instance.id");       w.Write("6b8d9f7c5-q8w2n");
        w.Write("telemetry.sdk.name");        w.Write("opentelemetry");
        w.Write("telemetry.sdk.language");    w.Write("dotnet");
        w.Write("telemetry.sdk.version");     w.Write("1.9.0");
        w.Write("http.route");                w.Write("/api/v2/payments/{id}/capture");
        w.Write("http.response.status_code"); w.Write(200);
        w.Write("@tr");                       w.Write("4bf92f3577b34da6a3ce929d0e0e4736");
        w.Write("@sp");                       w.Write("00f067aa0ba902b7");
        w.Flush();
        return buf.WrittenSpan.ToArray();
    }
}
