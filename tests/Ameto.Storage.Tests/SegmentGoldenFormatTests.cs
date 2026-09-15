using System.Buffers;
using System.Security.Cryptography;
using MessagePack;
using Ameto.Core;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// A GOLDEN HASH of one segment file, written from a tier with nothing environmental in it.
///
/// <para>Every other format test in this suite compares the writer against ITSELF — two routes,
/// two orders, a round-trip through the reader — and all of them stay green through a change
/// that moves every byte in the same direction. This one compares it against a CONSTANT taken
/// from a build that predates the change, which is the only way "the format did not move" is a
/// claim rather than a hope. The .seg format is the on-disk contract: a segment written by one
/// version is read by every later one, and <c>MergeCompressionProbe</c>, the block geometry
/// tests and the query path all pin numbers derived from it.</para>
///
/// <para>SCOPE: the writer's own bytes — header, blocks, all nine columns, block index, group
/// directory, footer. The index sections are excluded by writing with no sink, deliberately:
/// they are built by <c>Ameto.Indexing</c>, which is a different work package, and a golden hash
/// over them would fail for reasons that have nothing to do with the writer.</para>
///
/// <para>DETERMINISM is the whole value here, so the tier below contains no clock, no Guid and
/// no unseeded randomness: fixed ticks, fixed ids, arithmetic payloads, a fixed node and segment
/// id. LZ4 is deterministic for a given input and level. If this test ever becomes flaky it is
/// because something environmental crept into the tier, and that must be fixed rather than the
/// constant re-baselined.</para>
///
/// <para>If it fails after a deliberate format change: re-baseline the constant, say so in the
/// commit, and expect to bump <c>SegVersion</c> — an old reader will not understand the new
/// bytes.</para>
/// </summary>
public sealed class SegmentGoldenFormatTests
{
    private readonly ITestOutputHelper _out;
    public SegmentGoldenFormatTests(ITestOutputHelper o) => _out = o;

    /// <summary>
    /// SHA-256 of the segment <see cref="BuildDeterministicTier"/> produces, as written by
    /// commit 677b49f — the tip of <c>perf/logs-cpu-alloc</c>, i.e. the writer as it stood
    /// BEFORE this work package touched it (before 422de75 "the flush decides its file order
    /// in one pass over the headers"). Recomputed by checking those sources out over this
    /// worktree and running this test; it must not be edited to make a failure go away.
    /// </summary>
    private const string GoldenSha256 = "AFCF6BFB84CD72E3A6D2B37418B5F609B4835FCC24C1B06EC2DA7F497687CEA2";

    private const int    Events   = 4_000;
    private const long   BaseTicks = 640_000_000_000_000_000L;   // a fixed instant, not a clock

    [Fact]
    public void TheWriterStillProducesThePreChangeBytes()
    {
        string path = Path.Combine(Path.GetTempPath(), $"Ameto-golden-{Guid.NewGuid():N}.seg");
        try
        {
            var pool = new StringInternPool();
            byte[] bytes;
            using (var hot = BuildDeterministicTier(pool))
            using (var w = new SegmentWriter(path))
            {
                w.WriteEvents(hot, pool, SegmentWriter.ComputeSortOrder(hot));
                var info = w.Finalise(new NodeId(5), new SegmentId(77UL));
                Assert.Equal((uint)Events, info.EventCount);
            }
            bytes = File.ReadAllBytes(path);

            string actual = Convert.ToHexString(SHA256.HashData(bytes));
            _out.WriteLine($"segment {bytes.Length} bytes, sha256 {actual}");

            Assert.Equal(GoldenSha256, actual);
        }
        finally { File.Delete(path); }
    }

    /// <summary>
    /// A tier that is the same on every machine and every run. Deliberately varied across
    /// everything the writer encodes per row: both timestamp orders (the last tenth arrives out
    /// of order, so the sort runs rather than the identity path), several templates and service
    /// names so the memo hits and misses, every level, rows with and without trace/span ids,
    /// properties of three shapes, and exceptions with and without a stack and an inner chain.
    /// </summary>
    private static HotTierSegment BuildDeterministicTier(StringInternPool pool)
    {
        var hot = new HotTierSegment(Events + 1, (long)Events * 1024 + 1024 * 1024);

        string[] templates =
        [
            "HTTP {Method} {Route} responded {Status} in {Elapsed} ms",
            "cache {Key} miss",
            "payment {Id} settled for {Customer}",
        ];
        string?[] services = ["Wallet.API", "Ledger.Worker", null];
        int[] tmplIdx = new int[templates.Length];
        for (int t = 0; t < templates.Length; t++) tmplIdx[t] = pool.Intern(templates[t]);
        int[] svcIdx = new int[services.Length];
        for (int s = 0; s < services.Length; s++) svcIdx[s] = services[s] is null ? -1 : pool.Intern(services[s]!);

        var buf = new ArrayBufferWriter<byte>(512);
        for (int i = 0; i < Events; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            switch (i % 3)
            {
                case 0:
                    w.WriteMapHeader(4);
                    w.Write("orderId");    w.Write((long)i);
                    w.Write("customerId"); w.Write("cust-" + (i % 97));
                    w.Write("route");      w.Write("/api/pay");
                    w.Write("duration");   w.Write((i % 400) + 0.5);
                    break;
                case 1:
                    w.WriteMapHeader(2);
                    w.Write("key");    w.Write("k:" + (i % 31));
                    w.Write("hit");    w.Write(i % 2 == 0);
                    break;
                default:
                    w.WriteMapHeader(0);     // present but empty
                    break;
            }
            w.Flush();

            ExceptionInfo? exc = (i % 5) switch
            {
                0 => new ExceptionInfo { Type = "System.TimeoutException" },
                1 => new ExceptionInfo
                {
                    Type       = "System.InvalidOperationException",
                    Message    = "payment " + (i % 211) + " could not be settled",
                    StackTrace = "   at Ameto.Wallet.Handler.Step(Int32 n)\n   at Ameto.Wallet.Handler.Run()\n",
                },
                2 => new ExceptionInfo
                {
                    Type    = "System.AggregateException",
                    Message = "one or more errors",
                    Inner   = new ExceptionInfo { Type = "System.Net.Http.HttpRequestException", Message = "refused" },
                },
                _ => null,
            };

            // The last tenth arrives out of order, so ComputeSortOrder takes its sorting route.
            long ticks = i < Events - Events / 10
                ? BaseTicks + (long)i * 10_000
                : BaseTicks + (long)(Events - i) * 10_000;

            int t2 = i % templates.Length;
            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = ticks,
                Level                    = (LogLevel)(i % 6),
                MessageTemplatePoolIndex = tmplIdx[t2],
                ServiceNamePoolIndex     = svcIdx[i % services.Length],
                TraceIdHi                = i % 4 == 0 ? 0UL : (ulong)i * 0x9E3779B97F4A7C15UL,
                TraceIdLo                = i % 4 == 0 ? 0UL : (ulong)i * 0xBF58476D1CE4E5B9UL,
                SpanId                   = i % 3 == 0 ? 0UL : (ulong)i * 0x94D049BB133111EBUL,
            };
            Assert.True(hot.TryWrite(h, buf.WrittenSpan, pool.Get(tmplIdx[t2]), exc));
        }
        hot.Freeze();
        return hot;
    }
}
