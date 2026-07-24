using Ameto.Tracing;
using Ameto.Tracing.Storage;
using K4os.Compression.LZ4;
using MessagePack;

namespace Ameto.Storage.Tests;

/// <summary>
/// v3 .trc format: roundtrip fidelity (fields, typed attributes, root spans),
/// index-based lookups, legacy-v2 readability, and the size win over v2 on a
/// corpus shaped like real OTel spans.
/// </summary>
public sealed class SpanFormatV3Tests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-trc-" + Guid.NewGuid().ToString("N"));

    public SpanFormatV3Tests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    // ── Corpus (deterministic, realistic OTel shape) ──────────────────────────

    private static List<SpanRecord> Corpus(int traceCount, int spansPerTrace)
    {
        var rnd    = new Random(42);
        var routes = new[]
        {
            "console/api/{provider}/ProviderPayment/{providerPaymentId}",
            "api/payments/{id}/status", "api/kiosk/{kioskId}/heartbeat",
            "api/providers/{providerId}/balance",
        };
        var services = new[] { "MintRoute.API", "KioskAgent.API", "Etisalat.API" };
        var ops      = new[] { "GET {route}", "SELECT payments", "publish PaymentAccepted", "HTTP POST" };

        var spans = new List<SpanRecord>(traceCount * spansPerTrace);
        long baseTs = 1_784_800_000_000_000_000L;
        for (int t = 0; t < traceCount; t++)
        {
            var traceId = new TraceId((ulong)(t + 1) * 0x9E3779B97F4A7C15UL, (ulong)(t + 17) * 0xC2B2AE3D27D4EB4FUL);
            long traceStart = baseTs + t * 50_000_000L + rnd.Next(0, 1000) * 1_000L;
            ulong rootSid = (ulong)(t * 100 + 1);
            for (int i = 0; i < spansPerTrace; i++)
            {
                bool isRoot = i == 0;
                spans.Add(new SpanRecord
                {
                    TraceId           = traceId,
                    SpanId            = new SpanId(rootSid + (ulong)i),
                    ParentSpanId      = isRoot ? default : new SpanId(rootSid + (ulong)(i - 1) / 2),
                    StartTimeUnixNano = traceStart + i * 2_000_000L + rnd.Next(0, 500) * 1_000L,
                    DurationNanos     = 1_000_000L + rnd.Next(0, 60_000) * 1_000L,
                    Name              = ops[i % ops.Length],
                    ServiceName       = services[(t + i / 3) % services.Length],
                    Kind              = i == 0 ? SpanKind.Server : (i % 3 == 0 ? SpanKind.Client : SpanKind.Internal),
                    Status            = rnd.Next(0, 50) == 0 ? SpanStatusCode.Error : SpanStatusCode.Unset,
                    HttpStatusCode    = (short)(i == 0 ? 200 : 0),
                    // Resource attributes ride on EVERY span (the server merges them
                    // in at ingest), per-signal attrs on top — the real stored shape.
                    Attributes        = BuildAttrs(t, i, routes),
                });
            }
        }
        return spans;
    }

    private static Dictionary<string, object?>? BuildAttrs(int t, int i, string[] routes)
    {
        var d = new Dictionary<string, object?>
        {
            ["Environment"]         = "Development",
            ["PR"]                  = "pr7170",
            ["service.instance.id"] = new Guid(t % 7, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0).ToString(),
        };
        if (i == 0)
        {
            d["http.route"]               = routes[t % routes.Length];
            d["http.request.method"]      = "GET";
            d["http.response.status_code"] = 200L;
            d["network.protocol.version"] = "1.1";
            d["url.scheme"]               = "http";
            d["url.path"]                 = "/" + routes[t % routes.Length].Replace("{provider}", "prov" + t % 9);
        }
        else if (i % 3 == 0)
        {
            d["db.system"]    = "mssql";
            d["db.statement"] = "SELECT * FROM payments WHERE provider_id = @p" + (t % 5);
            d["db.rows"]      = 12.5 + i;
        }
        else
        {
            d["retry.count"] = (long)(i % 3);
            d["cache.hit"]   = i % 4 == 0;
        }
        return d;
    }

    // ── Roundtrip ─────────────────────────────────────────────────────────────

    [Fact]
    public void V3_Roundtrip_PreservesEverything()
    {
        var corpus = Corpus(50, 8); // 400 spans
        var info   = SpanWriter.Write(_dir, corpus);
        Assert.Equal(3, info.FormatVersion);
        Assert.Equal(corpus.Count, info.SpanCount);

        var readBack = SpanReader.ReadAll(info.FilePath);
        Assert.Equal(corpus.Count, readBack.Count);

        // Writer sorts by start time — compare against the same ordering.
        var expected = corpus.OrderBy(s => s.StartTimeUnixNano).ToList();
        for (int i = 0; i < expected.Count; i++)
        {
            var e = expected[i];
            var g = readBack[i];
            Assert.Equal(e.TraceId, g.TraceId);
            Assert.Equal(e.SpanId, g.SpanId);
            Assert.Equal(e.ParentSpanId, g.ParentSpanId);
            Assert.Equal(e.StartTimeUnixNano, g.StartTimeUnixNano); // exact nanos
            Assert.Equal(e.DurationNanos, g.DurationNanos);
            Assert.Equal(e.Name, g.Name);
            Assert.Equal(e.ServiceName, g.ServiceName);
            Assert.Equal(e.Kind, g.Kind);
            Assert.Equal(e.Status, g.Status);
            Assert.Equal(e.HttpStatusCode, g.HttpStatusCode);

            if (e.Attributes is null)
            {
                Assert.Null(g.Attributes);
            }
            else
            {
                Assert.NotNull(g.Attributes);
                Assert.Equal(e.Attributes.Count, g.Attributes!.Count);
                foreach (var (k, v) in e.Attributes)
                    Assert.Equal(v, g.Attributes[k]); // exact type + value (string/long/double/bool)
            }
        }
    }

    [Fact]
    public async Task V3_TraceIndex_And_ServiceIndex_Work()
    {
        var corpus = Corpus(120, 6); // multiple blocks? BlockSize=4096 — single block; still exercises indices
        var info   = SpanWriter.Write(_dir, corpus);

        // Trace-id lookup returns exactly that trace's spans.
        var target = corpus[37].TraceId;
        var expectedCount = corpus.Count(s => s.TraceId.Equals(target));
        int got = 0;
        await foreach (var s in SpanReader.ReadTraceAsync(info.FilePath, target, CancellationToken.None))
        {
            Assert.Equal(target, s.TraceId);
            got++;
        }
        Assert.Equal(expectedCount, got);

        // Service-filtered search matches a linear filter of the corpus.
        var svcExpected = corpus.Count(s => s.ServiceName == "MintRoute.API");
        int svcGot = 0;
        await foreach (var s in SpanReader.SearchAsync(info.FilePath, long.MinValue, long.MaxValue,
                           "MintRoute.API", null, null, null, null, null, null, CancellationToken.None))
        {
            Assert.Equal("MintRoute.API", s.ServiceName);
            svcGot++;
        }
        Assert.Equal(svcExpected, svcGot);
    }

    [Fact]
    public async Task V3_AttributeBloom_SkipsAndNeverDropsMatches()
    {
        var corpus = Corpus(80, 6);
        var info   = SpanWriter.Write(_dir, corpus);

        async Task<int> CountWithHints(params AttrHint[] hints)
        {
            int n = 0;
            await foreach (var _ in SpanReader.SearchAsync(info.FilePath, long.MinValue, long.MaxValue,
                               null, null, null, null, null, null, hints, CancellationToken.None)) n++;
            return n;
        }

        // Key-presence hint: every span carrying the key must come back.
        int withRoute = corpus.Count(s => s.Attributes?.ContainsKey("http.route") == true);
        Assert.True(withRoute > 0);
        // Bloom is a block-level pre-filter — it may keep whole blocks, never lose them.
        Assert.True(await CountWithHints(new AttrHint("http.route", null)) >= withRoute);

        // Value hint is case-insensitive (TraceQL OrdinalIgnoreCase semantics).
        Assert.True(await CountWithHints(new AttrHint("PR", "pr7170")) > 0);

        // A key that exists nowhere lets the bloom drop every block.
        Assert.Equal(0, await CountWithHints(new AttrHint("definitely.absent.key", null)));
        Assert.Equal(0, await CountWithHints(new AttrHint("PR", "no-such-value-xyz")));
    }

    [Fact]
    public void V2_LegacyFiles_StillReadable()
    {
        var corpus = Corpus(30, 6);
        var path   = Path.Combine(_dir, "legacy.trc");
        WriteV2File(path, corpus);

        var info = SpanReader.ReadSegmentInfo(path);
        Assert.Equal(2, info.FormatVersion);
        Assert.Equal(corpus.Count, info.SpanCount);

        var readBack = SpanReader.ReadAll(path);
        Assert.Equal(corpus.Count, readBack.Count);
        Assert.Equal(corpus[0].TraceId, readBack[0].TraceId);
        Assert.Equal(corpus[0].StartTimeUnixNano, readBack[0].StartTimeUnixNano);
        Assert.Equal(corpus[5].Name, readBack[5].Name);
    }

    [Fact]
    public void V3_IsMuchSmallerThanV2()
    {
        var corpus = Corpus(2000, 8); // 16k spans

        var v2Path = Path.Combine(_dir, "size-v2.trc");
        WriteV2File(v2Path, corpus);
        long v2Size = new FileInfo(v2Path).Length;

        var sub  = Path.Combine(_dir, "v3");
        Directory.CreateDirectory(sub);
        var info = SpanWriter.Write(sub, corpus);
        long v3Size = new FileInfo(info.FilePath).Length;

        Assert.True(v3Size * 3 <= v2Size * 2, // ≥ 1.5×
            $"expected v3 ≤ ⅔ of v2, got v2={v2Size:N0} B vs v3={v3Size:N0} B");
    }

    // ── Legacy v2 writer (copied from the pre-v3 SpanWriter, indices trimmed) ──

    private static void WriteV2File(string filePath, IList<SpanRecord> spans)
    {
        const int BlockSize = 4096;
        using var fs = new FileStream(filePath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 65536);
        using var bw = new BinaryWriter(fs);

        long minNano = spans.Min(s => s.StartTimeUnixNano);
        long maxNano = spans.Max(s => s.StartTimeUnixNano);

        bw.Write(0x52_44_54_43u); // "RDTC"
        bw.Write((ushort)2);
        bw.Write((uint)spans.Count);
        bw.Write(minNano);
        bw.Write(maxNano);
        bw.Write((byte)0);

        var traceIndex  = new Dictionary<TraceId, List<uint>>();
        var svcBlockMap = new Dictionary<string, SortedSet<uint>>(StringComparer.Ordinal);

        int written = 0;
        while (written < spans.Count)
        {
            int count = Math.Min(BlockSize, spans.Count - written);
            uint blockIdx = (uint)(written / BlockSize);

            var buf = new System.Buffers.ArrayBufferWriter<byte>();
            var w = new MessagePackWriter(buf);
            w.WriteArrayHeader(count);
            for (int i = 0; i < count; i++)
            {
                var s = spans[written + i];
                uint globalOffset = (uint)(written + i);
                if (!traceIndex.TryGetValue(s.TraceId, out var t)) traceIndex[s.TraceId] = t = new List<uint>(4);
                t.Add(globalOffset);
                if (!svcBlockMap.TryGetValue(s.ServiceName, out var blk)) svcBlockMap[s.ServiceName] = blk = new SortedSet<uint>();
                blk.Add(blockIdx);

                bool hasAttrs  = s.Attributes is { Count: > 0 };
                bool hasStatus = s.HttpStatusCode != 0;
                w.WriteMapHeader(9 + (hasAttrs ? 1 : 0) + (hasStatus ? 1 : 0));
                var idBuf = new byte[16];
                w.Write("tid"); s.TraceId.WriteTo(idBuf); w.Write(new ReadOnlySpan<byte>(idBuf));
                var sidBuf = new byte[8];
                w.Write("sid"); System.Buffers.Binary.BinaryPrimitives.WriteUInt64BigEndian(sidBuf, s.SpanId.RawValue); w.Write(new ReadOnlySpan<byte>(sidBuf));
                w.Write("pid"); System.Buffers.Binary.BinaryPrimitives.WriteUInt64BigEndian(sidBuf, s.ParentSpanId.RawValue); w.Write(new ReadOnlySpan<byte>(sidBuf));
                w.Write("ts");  w.Write(s.StartTimeUnixNano);
                w.Write("dur"); w.Write(s.DurationNanos);
                w.Write("n");   w.Write(s.Name);
                w.Write("svc"); w.Write(s.ServiceName);
                w.Write("k");   w.Write((byte)s.Kind);
                w.Write("st");  w.Write((byte)s.Status);
                if (hasStatus) { w.Write("hsc"); w.Write(s.HttpStatusCode); }
                if (hasAttrs)
                {
                    w.Write("attr");
                    var attrBytes = MessagePackSerializer.Serialize(
                        s.Attributes is Dictionary<string, object?> d ? d : new Dictionary<string, object?>(s.Attributes!));
                    w.Write(new ReadOnlySpan<byte>(attrBytes));
                }
            }
            w.Flush();
            var raw = buf.WrittenSpan.ToArray();
            var compressed = LZ4Pickler.Pickle(raw);
            bw.Write((uint)raw.Length);
            bw.Write((uint)compressed.Length);
            bw.Write(compressed);
            written += count;
        }

        long traceIdxOffset = fs.Position;
        bw.Write((uint)traceIndex.Count);
        var tid16 = new byte[16];
        foreach (var (traceId, offsets) in traceIndex)
        {
            traceId.WriteTo(tid16);
            bw.Write(tid16);
            bw.Write((uint)offsets.Count);
            foreach (var o in offsets) bw.Write(o);
        }

        long svcIdxOffset = fs.Position;
        bw.Write((uint)svcBlockMap.Count);
        foreach (var (svcName, blocks) in svcBlockMap)
        {
            var nameBytes = System.Text.Encoding.UTF8.GetBytes(svcName);
            bw.Write((ushort)nameBytes.Length);
            bw.Write(nameBytes);
            bw.Write((uint)blocks.Count);
            foreach (var b in blocks) bw.Write(b);
        }

        bw.Write((ulong)traceIdxOffset);
        bw.Write((ulong)svcIdxOffset);
        bw.Write(0x52_44_54_46u); // "RDTF"
    }
}
