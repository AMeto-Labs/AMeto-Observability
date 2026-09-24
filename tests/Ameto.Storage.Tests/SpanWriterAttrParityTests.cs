using System.Buffers;
using MessagePack;
using Microsoft.Extensions.Logging.Abstractions;
using Ameto.Tracing;
using Ameto.Tracing.Storage;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE GATE ON TS#1: the hot tier stopped decoding attributes on ingest, so the flush now writes
/// the msgpack map the OTLP mapper produced instead of re-encoding a dictionary it decoded. That
/// is allowed to be a byte-level change and is not allowed to be a format-level one, so every OTLP
/// value type goes through both writers here and the <c>.trc</c> bytes are compared.
///
/// <para>WHAT THE COMPARISON IS AGAINST MATTERS, and there are three writers in play, not two:</para>
/// <list type="number">
/// <item><b>blob</b> — <c>SpanWriter</c> copies <c>SpanRecord.AttributesBytes</c> through verbatim.
/// What this package ships.</item>
/// <item><b>lazy dictionary</b> — <c>SpanWriter.WriteAttributes</c> fed by
/// <c>SpanAttributeBlob.Decode</c>, the fallback the plan names. Identical to (1) on every scalar
/// value type; the theory below is what proves it.</item>
/// <item><b>main</b> — <c>SpanWriter.WriteAttributes</c> fed by
/// <c>MessagePackSerializer.Deserialize&lt;Dictionary&lt;string, object?&gt;&gt;</c>, which is what
/// the engine did on the ingest path before this change. NOT a parity baseline: that round trip is
/// LOSSY, and <see cref="The_pipeline_main_shipped_turned_a_port_number_into_a_string"/> is the
/// demonstration. <c>MessagePack</c>'s primitive formatter returns the narrowest integer type for
/// the encoding it met, so <c>1433</c> comes back as <c>ushort</c> — and <c>WriteAttributes</c> has
/// cases for <c>long</c>, <c>int</c>, <c>short</c> and <c>byte</c> but none for <c>ushort</c>, so
/// the value fell into <c>default: w.Write(v.ToString())</c> and was written to disk as the STRING
/// "1433". Every integer attribute in [256, 65535], in [65536, 2^32) and in [-128, -1] left the
/// hot tier with its type destroyed. Preserving those bytes was never an option worth having.</item>
/// </list>
/// </summary>
public sealed class SpanWriterAttrParityTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-attrparity-" + Guid.NewGuid().ToString("N"));
    private readonly ITestOutputHelper _out;

    public SpanWriterAttrParityTests(ITestOutputHelper output)
    {
        _out = output;
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, true); } catch { }
    }

    // ── The corpus ────────────────────────────────────────────────────────────

    /// <summary>
    /// One msgpack attribute map per case, built exactly the way <c>OtlpTraceMapper.WriteAnyValue</c>
    /// and <c>OtlpTraceStreamParser</c> build one: a map header, then typed values, then nothing.
    /// The integer cases are chosen by ENCODING WIDTH — positive fixint, uint8, uint16, uint32,
    /// int64, negative fixint, int8, int16, int32 — because the encoding is what decides which CLR
    /// type a decode hands back, and that is where a re-encode can go wrong.
    /// </summary>
    public static byte[] BlobFor(string name)
    {
        var buf = new ArrayBufferWriter<byte>(512);
        var w   = new MessagePackWriter(buf);

        switch (name)
        {
            case "string.ascii":     One(ref w, "db.system", static (ref MessagePackWriter x) => x.Write("mssql")); break;
            case "string.unicode":   One(ref w, "user.name", static (ref MessagePackWriter x) => x.Write("Ünïcödé — 数据库 🎉")); break;
            case "string.empty":     One(ref w, "db.name",   static (ref MessagePackWriter x) => x.Write("")); break;
            case "bool.true":        One(ref w, "cache.hit", static (ref MessagePackWriter x) => x.Write(true)); break;
            case "bool.false":       One(ref w, "cache.hit", static (ref MessagePackWriter x) => x.Write(false)); break;
            case "double":           One(ref w, "cpu.ratio", static (ref MessagePackWriter x) => x.Write(0.1)); break;
            case "double.negzero":   One(ref w, "cpu.ratio", static (ref MessagePackWriter x) => x.Write(-0.0)); break;
            case "double.nan":       One(ref w, "cpu.ratio", static (ref MessagePackWriter x) => x.Write(double.NaN)); break;
            case "double.infinity":  One(ref w, "cpu.ratio", static (ref MessagePackWriter x) => x.Write(double.PositiveInfinity)); break;
            case "int.fixpos":       One(ref w, "thread.id", static (ref MessagePackWriter x) => x.Write(63L)); break;
            case "int.uint8":        One(ref w, "thread.id", static (ref MessagePackWriter x) => x.Write(200L)); break;
            case "int.uint16":       One(ref w, "net.peer.port", static (ref MessagePackWriter x) => x.Write(1433L)); break;
            case "int.uint32":       One(ref w, "http.request.body.size", static (ref MessagePackWriter x) => x.Write(70_000L)); break;
            case "int.int64":        One(ref w, "db.rows", static (ref MessagePackWriter x) => x.Write(5_000_000_000L)); break;
            case "int.int64.min":    One(ref w, "db.rows", static (ref MessagePackWriter x) => x.Write(long.MinValue)); break;
            case "int.fixneg":       One(ref w, "retry.delta", static (ref MessagePackWriter x) => x.Write(-1L)); break;
            case "int.int8":         One(ref w, "retry.delta", static (ref MessagePackWriter x) => x.Write(-100L)); break;
            case "int.int16":        One(ref w, "retry.delta", static (ref MessagePackWriter x) => x.Write(-30_000L)); break;
            case "int.int32":        One(ref w, "retry.delta", static (ref MessagePackWriter x) => x.Write(-2_000_000_000L)); break;
            case "nil":              One(ref w, "tenant.id",  static (ref MessagePackWriter x) => x.WriteNil()); break;

            case "array":
                w.WriteMapHeader(1);
                w.Write("http.request.header.accept");
                w.WriteArrayHeader(3); w.Write("text/html"); w.Write(2L); w.Write(true);
                break;

            case "kvlist":
                w.WriteMapHeader(1);
                w.Write("peer.info");
                w.WriteMapHeader(2); w.Write("host"); w.Write("sql-03"); w.Write("port"); w.Write(1433L);
                break;

            case "duplicate.key":
                // The mapper writes RESOURCE attributes first and span attributes second, exactly
                // so a span attribute shadows a resource one of the same name.
                w.WriteMapHeader(2);
                w.Write("service.name"); w.Write("from-resource");
                w.Write("service.name"); w.Write("from-span");
                break;

            case "map16":
                // 20 pairs — past the fixmap boundary, so the map HEADER width is exercised too.
                w.WriteMapHeader(20);
                for (int i = 0; i < 20; i++) { w.Write($"attr.{i:00}"); w.Write((long)(i * 1000)); }
                break;

            case "sqlclient":
                w.WriteMapHeader(8);
                w.Write("db.system");     w.Write("mssql");
                w.Write("db.name");       w.Write("payments");
                w.Write("db.statement");  w.Write("SELECT TOP 100 Id, TenantId, Amount FROM dbo.Payments WHERE TenantId = @p0");
                w.Write("net.peer.name"); w.Write("sql-prod-03.svc.cluster.local");
                w.Write("net.peer.port"); w.Write(1433L);
                w.Write("http.route");    w.Write("/api/v1/tenants/{tenantId}/payments");
                w.Write("thread.id");     w.Write(17L);
                w.Write("otel.library.name"); w.Write("OpenTelemetry.Instrumentation.SqlClient");
                break;

            default: throw new ArgumentOutOfRangeException(nameof(name), name, "unknown attribute case");
        }

        w.Flush();
        return buf.WrittenMemory.ToArray();
    }

    private delegate void WriteValue(ref MessagePackWriter w);

    private static void One(ref MessagePackWriter w, string key, WriteValue value)
    {
        w.WriteMapHeader(1);
        w.Write(key);
        value(ref w);
    }

    // ── The parity theory ─────────────────────────────────────────────────────

    /// <summary>
    /// Every scalar OTLP value type: writing the blob through verbatim and re-encoding the
    /// dictionary it decodes to produce THE SAME .trc BYTES — header, blocks, indices, blooms and
    /// footer. That is the claim that makes "the hot tier keeps the blob" a byte-level change.
    /// </summary>
    [Theory]
    [InlineData("string.ascii")]
    [InlineData("string.unicode")]
    [InlineData("string.empty")]
    [InlineData("bool.true")]
    [InlineData("bool.false")]
    [InlineData("double")]
    [InlineData("double.negzero")]
    [InlineData("double.nan")]
    [InlineData("double.infinity")]
    [InlineData("int.fixpos")]
    [InlineData("int.uint8")]
    [InlineData("int.uint16")]
    [InlineData("int.uint32")]
    [InlineData("int.int64")]
    [InlineData("int.int64.min")]
    [InlineData("int.fixneg")]
    [InlineData("int.int8")]
    [InlineData("int.int16")]
    [InlineData("int.int32")]
    [InlineData("nil")]
    [InlineData("map16")]
    [InlineData("sqlclient")]
    public void A_scalar_attribute_map_writes_the_same_bytes_from_the_blob_as_from_its_dictionary(string name)
    {
        var blob = BlobFor(name);

        byte[] fromBlob = WriteOne(SpanFromBlob(blob));
        byte[] fromDict = WriteOne(SpanFromDictionary(blob));

        _out.WriteLine($"{name,-22} blob {blob.Length,4} B → .trc {fromBlob.Length,5} B / {fromDict.Length,5} B");

        Assert.Equal(fromDict, fromBlob);
    }

    /// <summary>
    /// The three cases where the two writers CANNOT agree, named one by one so that the set is
    /// pinned and a fourth cannot appear unnoticed. In all three the dictionary is the lossy side:
    /// a decode that has no CLR shape for an array or a nested map yields null, and a dictionary
    /// cannot hold a key twice. Copying the blob through is what keeps the value.
    /// </summary>
    [Theory]
    [InlineData("array")]
    [InlineData("kvlist")]
    [InlineData("duplicate.key")]
    public void A_value_no_dictionary_can_hold_is_kept_by_the_blob_and_lost_by_the_dictionary(string name)
    {
        var blob = BlobFor(name);

        byte[] fromBlob = WriteOne(SpanFromBlob(blob));
        byte[] fromDict = WriteOne(SpanFromDictionary(blob));

        Assert.NotEqual(fromDict, fromBlob);

        // And the blob side is the one that still holds what arrived.
        var blobSpan = ReadBackOne(SpanFromBlob(blob));
        var dictSpan = ReadBackOne(SpanFromDictionary(blob));

        if (name == "duplicate.key")
        {
            // Both sides answer "from-span" — the mapper's documented last-wins — but the
            // dictionary side dropped the shadowed pair on the way to disk and the blob side
            // still carries it.
            Assert.Equal("from-span", blobSpan.Attributes!["service.name"]);
            Assert.Equal("from-span", dictSpan.Attributes!["service.name"]);
            Assert.True(fromBlob.Length > fromDict.Length,
                "the blob keeps both copies of the key, so its segment is the larger one");
        }
        else
        {
            // The dictionary path wrote nil where the value used to be; the blob path wrote the
            // value. Neither becomes a CLR object — the block decoder boxes an array as null —
            // but only one of them can still be read by anything that understands msgpack.
            Assert.Null(dictSpan.Attributes!.GetValueOrDefault("http.request.header.accept")
                     ?? dictSpan.Attributes!.GetValueOrDefault("peer.info"));
            Assert.True(fromBlob.Length > fromDict.Length,
                "the blob keeps the nested value, so its segment is the larger one");
        }

        _out.WriteLine($"{name,-22} blob .trc {fromBlob.Length,5} B vs dictionary .trc {fromDict.Length,5} B");
    }

    // ── What main shipped, and why it is not the baseline ─────────────────────

    /// <summary>
    /// THE REGRESSION TEST FOR THE ENGINE PATH, end to end: a port number goes in through
    /// <c>WriteSpan</c>, the tier is flushed, and the segment is read back. It must come back as
    /// the number 1433.
    ///
    /// <para>Revert <c>AddToHotTierLocked</c> to <c>DeserializeAttributes(item.AttributesBytes)</c>
    /// and this fails with <c>"1433"</c> (a string): the primitive formatter hands the value back
    /// as <c>ushort</c>, <c>WriteAttributes</c> has no case for it, and the span is written to disk
    /// with its type replaced by <c>ToString()</c>. Every port, content length and small negative
    /// integer that ever passed through the hot tier was stored this way.</para>
    /// </summary>
    [Fact]
    public void The_pipeline_main_shipped_turned_a_port_number_into_a_string()
    {
        string dir = Path.Combine(_dir, "engine");
        Directory.CreateDirectory(dir);

        using (var engine = new TraceStorageEngine(dir, NullLogger<TraceStorageEngine>.Instance))
        {
            long baseNano = new DateTimeOffset(2026, 8, 1, 12, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds() * 1_000_000L;
            engine.WriteSpan(new SpanIngestItem
            {
                TraceId           = new TraceId(0x9E3779B97F4A7C15UL, 1),
                SpanId            = new SpanId(1),
                StartTimeUnixNano = baseNano,
                DurationNanos     = 1_000_000,
                Name              = "SELECT payments",
                ServiceName       = "billing",
                Kind              = SpanKind.Client,
                Status            = SpanStatusCode.Unset,
                AttributesBytes   = BlobFor("sqlclient"),
            });
            engine.FlushHotTier();
            engine.WaitForFlushForTest();
        }

        string trc = Assert.Single(Directory.GetFiles(dir, "*.trc"));
        var read   = Assert.Single(SpanReader.ReadAll(trc));

        _out.WriteLine($"net.peer.port read back as {read.Attributes!["net.peer.port"]!.GetType().Name} "
                     + $"= {read.Attributes["net.peer.port"]}");

        Assert.Equal(1433L, read.Attributes["net.peer.port"]);
        Assert.Equal("mssql", read.Attributes["db.system"]);
        Assert.Equal(17L, read.Attributes["thread.id"]);
    }

    /// <summary>
    /// A BLOB THAT IS NOT EXACTLY ONE MAP IS NEVER COPIED THROUGH — the bytes after a span's
    /// attributes are the next span's, so a verbatim copy of a truncated map, or of a map with one
    /// stray byte behind it, desynchronises the whole block and takes the other 4 095 spans in it
    /// with it. The writer only copies when its single validating walk reaches the end of the blob
    /// exactly; anything else falls back to re-encoding whatever the decode could recover.
    ///
    /// <para>Delete the <c>TryAddAttrBlobToBloom</c> guard in <c>SpanWriter</c> (write the blob
    /// unconditionally) and this fails on the stray-byte span: the block no longer decodes into
    /// three spans, and the run ends in a <c>MessagePackSerializationException</c> rather than an
    /// assertion.</para>
    /// </summary>
    [Fact]
    public void A_blob_that_is_not_one_whole_map_is_never_copied_through()
    {
        var good = BlobFor("sqlclient");
        var torn = good[..(good.Length / 2)];                   // truncated mid-value
        var tail = good.Concat(new byte[] { 0xc0 }).ToArray();  // a whole map plus a stray nil

        var spans = new List<SpanRecord>
        {
            SpanFromBlob(torn, id: 1),
            SpanFromBlob(tail, id: 2),
            SpanFromBlob(good, id: 3),
        };

        string path = SpanWriter.Write(_dir, spans).FilePath;
        var back    = SpanReader.ReadAll(path).OrderBy(s => s.SpanId.RawValue).ToList();

        // THE BLOCK SURVIVED. Three spans in, three spans out, in order, with their scalars.
        Assert.Equal(3, back.Count);
        Assert.Equal(new ulong[] { 1, 2, 3 }, back.Select(s => s.SpanId.RawValue).ToArray());

        Assert.Null(back[0].Attributes);                            // torn: nothing to recover
        Assert.Equal("mssql", back[1].Attributes!["db.system"]);     // stray byte: re-encoded, value kept
        Assert.Equal(1433L,   back[1].Attributes!["net.peer.port"]);
        Assert.Equal("mssql", back[2].Attributes!["db.system"]);     // and the good one is verbatim
    }

    // ── Plumbing ──────────────────────────────────────────────────────────────

    private static SpanRecord SpanFromBlob(byte[] blob, ulong id = 1) => new()
    {
        TraceId           = new TraceId(0x9E3779B97F4A7C15UL, id),
        SpanId            = new SpanId(id),
        StartTimeUnixNano = 1_754_049_600_000_000_000L + (long)id,
        DurationNanos     = 1_000_000,
        Name              = "SELECT payments",
        ServiceName       = "billing",
        Kind              = SpanKind.Client,
        Status            = SpanStatusCode.Unset,
        AttributesBytes   = blob,
    };

    /// <summary>The fallback the plan names: <c>WriteAttributes</c> fed by a lazily-decoded map.</summary>
    private static SpanRecord SpanFromDictionary(byte[] blob, ulong id = 1) => new()
    {
        TraceId           = new TraceId(0x9E3779B97F4A7C15UL, id),
        SpanId            = new SpanId(id),
        StartTimeUnixNano = 1_754_049_600_000_000_000L + (long)id,
        DurationNanos     = 1_000_000,
        Name              = "SELECT payments",
        ServiceName       = "billing",
        Kind              = SpanKind.Client,
        Status            = SpanStatusCode.Unset,
        Attributes        = SpanAttributeBlob.Decode(blob),
    };

    /// <summary>Writes one span to its own directory and returns the .trc bytes.</summary>
    private byte[] WriteOne(SpanRecord span)
    {
        string dir = Path.Combine(_dir, "w-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        string path = SpanWriter.Write(dir, new[] { span }).FilePath;
        return File.ReadAllBytes(path);
    }

    private SpanRecord ReadBackOne(SpanRecord span)
    {
        string dir = Path.Combine(_dir, "r-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        string path = SpanWriter.Write(dir, new[] { span }).FilePath;
        return SpanReader.ReadAll(path).Single();
    }
}
