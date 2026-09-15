using System.Buffers;
using System.Diagnostics;
using System.Globalization;
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
///
/// <para><c>Elapsed &gt; 20000</c> is measured twice: once against a number, which is the
/// unboxed numeric road, and once against the SAME values sent as strings, the way some OTLP
/// SDKs send <c>http.status_code</c>. The second shape is the numeric road's known trade-off.
/// The number probe walks the msgpack map, finds a string, declines, and the general road walks
/// the map a second time to decode it, so every candidate event pays two key walks where it once
/// paid one. The row exists so that cost is a number rather than a guess.</para>
/// </summary>
public sealed class FilterEvalProbe
{
    private const int Events = 20_000;

    private readonly ITestOutputHelper _out;
    public FilterEvalProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public void MatchesPerEventCost()
    {
        var ascii       = BuildEvents(unicode: false);
        var unicode     = BuildEvents(unicode: true);
        var textNumbers = BuildEvents(unicode: false, elapsedAsText: true);

        // EVERY expression is walked before ANY of them is timed. Tier-1 promotion is
        // asynchronous, and the evaluator is one shared set of methods, so whichever
        // expression was reported first used to be measured while the JIT was still catching
        // up with it — its line came out several times worse than the same code measured
        // later, which reads exactly like a regression and is not one.
        foreach (var (expr, events) in Expressions(ascii, unicode, textNumbers))
        {
            var warm = CompiledFilter.Compile(expr);
            for (int i = 0; i < 5; i++)
                foreach (var ev in events) if (warm.Matches(ev)) { }
        }

        Report("(a) @l = 'Error' and @mt like '%timeout%'", "@l = 'Error' and @mt like '%timeout%'", ascii);
        Report("    @mt like '%timeout%'",                 "@mt like '%timeout%'",                 ascii);
        Report("    @mt like 'Handled%'",                  "@mt like 'Handled%'",                  ascii);
        Report("    @mt like '%request'",                  "@mt like '%request'",                  ascii);
        Report("    @l = 'Error'",                         "@l = 'Error'",                         ascii);
        Report("    RequestPath like '%users%'",           "RequestPath like '%users%'",           ascii);
        Report("    Elapsed > 20000",                      "Elapsed > 20000",                      ascii);
        Report("    Elapsed > 20000  (string-valued)",     "Elapsed > 20000",                      textNumbers);
        Report("    @mt like '%timeout%'  (non-ASCII)",    "@mt like '%timeout%'",                 unicode);
    }

    /// <summary>The measured set, in one place, so the warm-up cannot fall out of step with it.</summary>
    private static (string Expression, List<LogEvent> Events)[] Expressions(
        List<LogEvent> ascii, List<LogEvent> unicode, List<LogEvent> textNumbers) =>
    [
        ("@l = 'Error' and @mt like '%timeout%'", ascii),
        ("@mt like '%timeout%'",                  ascii),
        ("@mt like 'Handled%'",                   ascii),
        ("@mt like '%request'",                   ascii),
        ("@l = 'Error'",                          ascii),
        ("RequestPath like '%users%'",            ascii),
        ("Elapsed > 20000",                       ascii),
        ("Elapsed > 20000",                       textNumbers),
        ("@mt like '%timeout%'",                  unicode),
    ];

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
    /// <param name="elapsedAsText">
    /// Writes <c>Elapsed</c> as a msgpack STRING of the same number. Same seed, same key order,
    /// same map size, so the only difference from the numeric set is the value's type.
    /// </param>
    private static List<LogEvent> BuildEvents(bool unicode, bool elapsedAsText = false)
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
            int elapsed = rng.Next(1, 40_000);
            w.Write("Elapsed");
            if (elapsedAsText) w.Write(elapsed.ToString(CultureInfo.InvariantCulture));
            else               w.Write(elapsed);
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
