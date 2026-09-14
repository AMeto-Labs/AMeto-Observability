using System.Buffers;
using System.Diagnostics;
using Ameto.Core;
using Ameto.Query.Filtering;
using MessagePack;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// Nanoseconds per <c>FilterEvaluator.Matches</c> — the cost paid once per CANDIDATE event,
/// which on a wide window is hundreds of thousands of times per query even though only a few
/// hundred rows come back.
///
/// <para>The shapes measured are the ones the log list actually produces: the recon's query
/// (a) <c>@l = 'Error' and @mt like '%timeout%'</c>, its two halves on their own, and the
/// same contains-search against a user property so the property-probe road is in the numbers
/// too. A non-ASCII value is measured beside the ASCII one because the vectorised
/// contains/startsWith/endsWith path is only taken when BOTH the pattern literal and the value
/// are ASCII — everything else keeps the folding matcher, and this probe says what that
/// fallback costs.</para>
/// </summary>
public sealed class FilterEvalProbe
{
    private const int Events = 20_000;

    private readonly ITestOutputHelper _out;
    public FilterEvalProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void MatchesPerEventCost()
    {
        var ascii    = BuildEvents(unicode: false);
        var unicode  = BuildEvents(unicode: true);

        Report("(a) @l = 'Error' and @mt like '%timeout%'", "@l = 'Error' and @mt like '%timeout%'", ascii);
        Report("    @mt like '%timeout%'",                 "@mt like '%timeout%'",                 ascii);
        Report("    @mt like 'Handled%'",                  "@mt like 'Handled%'",                  ascii);
        Report("    @mt like '%request'",                  "@mt like '%request'",                  ascii);
        Report("    @l = 'Error'",                         "@l = 'Error'",                         ascii);
        Report("    RequestPath like '%users%'",           "RequestPath like '%users%'",           ascii);
        Report("    @mt like '%timeout%'  (non-ASCII)",    "@mt like '%timeout%'",                 unicode);
    }

    /// <summary>
    /// The BEST of several passes, not the average.
    ///
    /// <para>A per-event cost of tens of nanoseconds cannot be read off a mean on a machine
    /// that is also running a build: one descheduled pass moves the average by more than the
    /// whole effect being measured, and back-to-back runs of the unchanged control swung 13 ns
    /// to 415 ns here. Interference can only ADD time, so the minimum over repetitions is the
    /// one statistic that converges on what the code costs rather than on what else the machine
    /// was doing. Allocation is reported as the mean because
    /// GC.GetAllocatedBytesForCurrentThread is exact and load does not move it.</para>
    /// </summary>
    private void Report(string label, string expression, List<LogEvent> events)
    {
        var filter = CompiledFilter.Compile(expression);

        int hits = 0;
        for (int i = 0; i < 3; i++)
            foreach (var ev in events) if (filter.Matches(ev)) hits++;

        const int reps   = 15;
        const int rounds = 3;

        GC.Collect();
        GC.WaitForPendingFinalizers();
        long b0 = GC.GetAllocatedBytesForCurrentThread();

        double best = double.MaxValue;
        int matched = 0;
        for (int rep = 0; rep < reps; rep++)
        {
            var sw = Stopwatch.StartNew();
            int local = 0;
            for (int r = 0; r < rounds; r++)
                foreach (var ev in events) if (filter.Matches(ev)) local++;
            sw.Stop();
            matched = local;
            double ns = sw.Elapsed.TotalMilliseconds * 1_000_000.0 / (rounds * (double)events.Count);
            if (ns < best) best = ns;
        }

        long bytes = GC.GetAllocatedBytesForCurrentThread() - b0;
        double bytesPerEvent = bytes / (double)(reps * rounds * events.Count);

        _out.WriteLine($"{label,-46} {best,8:F1} ns/event  {bytesPerEvent,7:F2} B/event  ({matched / rounds} hits)");
        Assert.True(hits >= 0);
    }

    /// <summary>
    /// 1 % Error, 10 % carrying "timeout" somewhere in the template, a per-event
    /// <c>RequestPath</c> property, templates around 100 characters — the density the recon
    /// used to size query (a).
    /// </summary>
    private static List<LogEvent> BuildEvents(bool unicode)
    {
        var list = new List<LogEvent>(Events);
        var rng  = new Random(12345);
        string pad = unicode
            ? " — обработчик отработал, ответ отправлен клиенту, соединение закрыто штатно"
            : " -- the handler completed, the response was written and the connection closed";

        for (int i = 0; i < Events; i++)
        {
            bool slow  = (i % 10) == 0;
            bool error = (i % 100) == 0;
            string template = slow
                ? "Command MergeCreateCommand hit a timeout after 30s" + pad
                : "Command MergeCreateCommand handled; response returned" + pad;

            var buf = new ArrayBufferWriter<byte>(256);
            var w = new MessagePackWriter(buf);
            w.WriteMapHeader(3);
            w.Write("SourceContext"); w.Write("Common.MediatR.LoggingBehavior");
            w.Write("RequestPath");   w.Write(slow ? "/api/v1/users/" + rng.Next(1000) : "/api/v1/orders/" + rng.Next(1000));
            w.Write("Elapsed");       w.Write(rng.Next(1, 40_000));
            w.Flush();

            list.Add(new LogEvent
            {
                Id              = new EventId(0u, (uint)i),
                Timestamp       = DateTimeOffset.UtcNow.AddSeconds(-i),
                Level           = error ? Ameto.Core.LogLevel.Error : Ameto.Core.LogLevel.Information,
                MessageTemplate = template,
                ServiceName     = "Office.API",
                RawProperties   = buf.WrittenMemory,
            });
        }
        return list;
    }
}
