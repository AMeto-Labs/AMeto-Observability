using Ameto.Core;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// DIAGNOSTIC PROBE (temporary). Compares candidate build-phase representations for
/// <c>SegmentTrigramIndex</c>, which the breakdown probe identified as the dominant
/// flush-path memory holder (~324 MB of ~400 MB for a 64 MB tier).
///
/// <para>The key property the current <c>HashSet&lt;int&gt;</c> ignores: within one flush,
/// <c>SegmentIndexBuilder.Build</c> walks <c>pos = 0..hot.Count</c> and passes
/// <c>offset = pos</c>, so offsets arrive MONOTONICALLY ASCENDING. The set is therefore
/// only ever de-duplicating repeats of the CURRENT offset (the same trigram appearing in
/// both the template and a property value of the same event) — exactly the case
/// <c>SegmentInvertedIndex</c> already handles with a <c>list[^1] != offset</c> check.</para>
/// </summary>
public sealed class TrigramAccumulatorProbe
{
    private readonly ITestOutputHelper _out;
    public TrigramAccumulatorProbe(ITestOutputHelper o) => _out = o;

    private const int Events = 130_000;
    private const double MB  = 1048576.0;

    [Fact]
    public void TrigramAccumulator_Candidates()
    {
        var texts = GenerateTexts(Events);
        long pairs = CountPairs(texts);

        _out.WriteLine($"{Events:N0} events, {pairs:N0} (trigram, offset) pairs after dedup\n");

        long baseline = Measure(texts, static () => new HashSetAccumulator());
        long lists    = Measure(texts, static () => new SortedListAccumulator());
        long varint   = Measure(texts, static () => new DeltaVarintAccumulator());

        Report("A. HashSet<int>            (current)", baseline, pairs, baseline);
        Report("B. List<int> + last-check  ", lists,  pairs, baseline);
        Report("C. delta-varint bytes      ", varint, pairs, baseline);
    }

    private void Report(string label, long bytes, long pairs, long baseline)
        => _out.WriteLine($"  {label}  {bytes / MB,7:F1} MB   {(double)bytes / pairs,5:F1} B/pair   {(double)baseline / bytes,5:F1}x smaller");

    private static long Measure(string[][] texts, Func<IAccumulator> factory)
    {
        long before = GC.GetTotalMemory(true);
        var acc = factory();
        for (int i = 0; i < texts.Length; i++)
            foreach (var t in texts[i])
                acc.AddText((uint)i, t);
        long after = GC.GetTotalMemory(true);
        GC.KeepAlive(acc);
        return after - before;
    }

    private static long CountPairs(string[][] texts)
    {
        var acc = new SortedListAccumulator();
        long n = 0;
        for (int i = 0; i < texts.Length; i++)
            foreach (var t in texts[i])
                n += acc.AddText((uint)i, t);
        return n;
    }

    // ── candidate accumulators ────────────────────────────────────────────────

    private interface IAccumulator { int AddText(uint offset, string text); }

    /// <summary>What SegmentTrigramIndex does today.</summary>
    private sealed class HashSetAccumulator : IAccumulator
    {
        private readonly Dictionary<(char, char, char), HashSet<int>> _sets = new();
        public int AddText(uint offset, string text)
        {
            if (text.Length < 3) return 0;
            Span<char> lower = stackalloc char[text.Length];
            int n = text.AsSpan().ToLowerInvariant(lower);
            int added = 0;
            for (int i = 0; i <= n - 3; i++)
            {
                var key = (lower[i], lower[i + 1], lower[i + 2]);
                if (!_sets.TryGetValue(key, out var set)) { set = new HashSet<int>(); _sets[key] = set; }
                if (set.Add((int)offset)) added++;
            }
            return added;
        }
    }

    /// <summary>Monotonic offsets ⇒ a plain ascending list with a last-element check
    /// is semantically identical to the set, at a fraction of the per-entry cost.</summary>
    private sealed class SortedListAccumulator : IAccumulator
    {
        private readonly Dictionary<(char, char, char), List<int>> _lists = new();
        public int AddText(uint offset, string text)
        {
            if (text.Length < 3) return 0;
            Span<char> lower = stackalloc char[text.Length];
            int n = text.AsSpan().ToLowerInvariant(lower);
            int added = 0;
            for (int i = 0; i <= n - 3; i++)
            {
                var key = (lower[i], lower[i + 1], lower[i + 2]);
                if (!_lists.TryGetValue(key, out var list)) { list = new List<int>(); _lists[key] = list; }
                if (list.Count == 0 || list[^1] != (int)offset) { list.Add((int)offset); added++; }
            }
            return added;
        }
    }

    /// <summary>Ascending offsets encode as gaps; a dense trigram (present on nearly every
    /// event) costs ~1 byte per event instead of a 4-byte int plus set overhead.</summary>
    private sealed class DeltaVarintAccumulator : IAccumulator
    {
        private sealed class Posting
        {
            public byte[] Buf = new byte[8];
            public int    Len;
            public int    Last = -1;
        }

        private readonly Dictionary<(char, char, char), Posting> _p = new();

        public int AddText(uint offset, string text)
        {
            if (text.Length < 3) return 0;
            Span<char> lower = stackalloc char[text.Length];
            int n = text.AsSpan().ToLowerInvariant(lower);
            int added = 0;
            for (int i = 0; i <= n - 3; i++)
            {
                var key = (lower[i], lower[i + 1], lower[i + 2]);
                if (!_p.TryGetValue(key, out var post)) { post = new Posting(); _p[key] = post; }
                if (post.Last == (int)offset) continue;

                uint gap = (uint)((int)offset - post.Last);   // Last == -1 ⇒ gap = offset + 1
                post.Last = (int)offset;
                if (post.Len + 5 > post.Buf.Length)
                    Array.Resize(ref post.Buf, post.Buf.Length * 2);
                while (gap >= 0x80) { post.Buf[post.Len++] = (byte)(gap | 0x80); gap >>= 7; }
                post.Buf[post.Len++] = (byte)gap;
                added++;
            }
            return added;
        }
    }

    // ── data: what SegmentIndexBuilder actually feeds the trigram index ───────

    private static string[][] GenerateTexts(int n)
    {
        const string template = "HTTP {Method} {Route} responded {Status} in {Elapsed} ms";
        var rng = new Random(5);
        string[] methods = { "GET", "POST", "PUT", "DELETE" };
        string[] routes  = { "/api/pay", "/api/topup", "/api/status", "/api/balance" };
        var arr = new string[n][];
        for (int i = 0; i < n; i++)
            arr[i] =
            [
                template,                                                   // @mt
                TraceIdHelper.FormatTraceId((ulong)rng.NextInt64(), (ulong)rng.NextInt64())!,
                TraceIdHelper.FormatSpanId((ulong)rng.NextInt64())!,
                "cust-" + rng.Next(0, 100_000),
                methods[rng.Next(methods.Length)],
                routes[rng.Next(routes.Length)],
                "ae-dxb",
                "0HN" + rng.Next().ToString("x"),
            ];
        return arr;
    }
}
