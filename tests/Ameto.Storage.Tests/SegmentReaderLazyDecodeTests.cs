using System.Buffers;
using Ameto.Core;
using MessagePack;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// WHAT A CANDIDATE ROW COSTS BEFORE THE FILTER HAS SEEN IT.
///
/// <para>A trigram hint produces a SUPERSET of the matches, so most rows a cold scan decodes
/// are about to be rejected. The decoder used to hand each of them a fully built
/// <see cref="ExceptionInfo"/> tree — a stack trace is 1-5 KB and a level-split Error segment
/// is ~100 % exception-bearing — and a freshly transcoded template and service string, even
/// though a block of 2 000 rows is drawn from a vocabulary of a few dozen.</para>
///
/// <para>These tests pin the two properties that make that stop: the exception is not decoded
/// until something asks for it, and equal strings inside one read are ONE instance. Both are
/// invisible to a result-equality test, which is why they are asserted directly — the
/// behaviour they protect is the allocation, and the results must not move at all.</para>
/// </summary>
public sealed class SegmentReaderLazyDecodeTests : IDisposable
{
    private const int Events    = 600;
    private const int Templates = 6;

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ameto-lazydecode-" + Guid.NewGuid().ToString("N"));

    public SegmentReaderLazyDecodeTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, true); } catch { } }

    [Fact]
    public async Task Exception_is_not_decoded_until_it_is_asked_for()
    {
        string path = WriteSegment();
        using var reader = SegmentReader.Open(path);

        int seen = 0;
        await foreach (var ev in reader.ReadEventsAsync(null, null, null))
        {
            // The row carries an exception and knows so WITHOUT building one.
            Assert.True(ev.HasException);
            Assert.False(ev.ExceptionMaterialised);

            // …and decodes to exactly what was written, once asked.
            var ex = ev.Exception;
            Assert.NotNull(ex);
            Assert.True(ev.ExceptionMaterialised);
            Assert.Equal("System.InvalidOperationException", ex!.Type);
            Assert.Equal("boom " + seen % Templates, ex.Message);
            Assert.NotNull(ex.StackTrace);
            Assert.Equal("System.FormatException", ex.Inner?.Type);

            // Second read is the same instance — decoded once, not once per touch.
            Assert.Same(ex, ev.Exception);
            seen++;
        }

        Assert.Equal(Events, seen);
    }

    [Fact]
    public async Task Repeated_templates_and_service_names_are_one_string_per_read()
    {
        string path = WriteSegment();
        using var reader = SegmentReader.Open(path);

        var templates = new Dictionary<string, string>(StringComparer.Ordinal);
        string? service = null;
        int rows = 0;

        await foreach (var ev in reader.ReadEventsAsync(null, null, null))
        {
            if (templates.TryGetValue(ev.MessageTemplate, out var first))
                Assert.Same(first, ev.MessageTemplate);
            else
                templates[ev.MessageTemplate] = ev.MessageTemplate;

            Assert.NotNull(ev.ServiceName);
            service ??= ev.ServiceName;
            Assert.Same(service, ev.ServiceName);
            rows++;
        }

        Assert.Equal(Events, rows);
        Assert.Equal(Templates, templates.Count);
    }

    /// <summary>A segment of <see cref="Events"/> exception-bearing rows over
    /// <see cref="Templates"/> distinct templates and one service name.</summary>
    private string WriteSegment()
    {
        var pool = new StringInternPool();
        int svcIdx = pool.Intern("Svc.A");

        var tmplIdx = new int[Templates];
        for (int t = 0; t < Templates; t++) tmplIdx[t] = pool.Intern($"order {{Id}} failed in stage {t}");

        using var hot = new HotTierSegment(Events + 1, (long)Events * 4096 + (1 << 20));
        long baseTicks = DateTimeOffset.UtcNow.UtcTicks;

        var buf = new ArrayBufferWriter<byte>(256);
        for (int i = 0; i < Events; i++)
        {
            buf.Clear();
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(1);
            w.Write("orderId"); w.Write((long)i);
            w.Flush();

            int t = i % Templates;
            var exc = new ExceptionInfo
            {
                Type       = "System.InvalidOperationException",
                Message    = "boom " + t,
                StackTrace = new string('f', 1200),   // a realistic stack trace, not a token
                Inner      = new ExceptionInfo { Type = "System.FormatException", Message = "inner" },
            };

            var h = new LogEventHeader
            {
                Id                       = new EventId(0u, (uint)i).RawValue,
                TimestampUtcTicks        = baseTicks + i,
                Level                    = LogLevel.Error,
                MessageTemplatePoolIndex = tmplIdx[t],
                ServiceNamePoolIndex     = svcIdx,
                HasException             = true,
            };
            Assert.True(hot.TryWrite(h, buf.WrittenSpan, pool.Get(tmplIdx[t]), exc));
        }
        hot.Freeze();

        string path = Path.Combine(_dir, "lazy.seg");
        var order = SegmentWriter.ComputeSortOrder(hot);
        using (var sw = new SegmentWriter(path))
        {
            sw.WriteEvents(hot, pool, order);
            sw.Finalise(new NodeId(0), new SegmentId(11UL));
        }
        return path;
    }
}
