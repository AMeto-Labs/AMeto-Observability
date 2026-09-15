using Ameto.Indexing;

namespace Ameto.Indexing.Tests;

/// <summary>
/// THE ANSWERS, while <see cref="SegmentTrigramIndex.Lookup"/> changed shape underneath them.
///
/// <para>Lookup used to fold the search text through <c>ToString().ToLowerInvariant()</c>, seed a
/// <c>HashSet&lt;int&gt;</c> from the first trigram's posting list, <c>IntersectWith</c> each
/// further one and finish with <c>ToArray</c> + <c>Array.Sort</c> + <c>Array.ConvertAll</c>. It is
/// a two-pointer merge over the sorted posting lists now, rarest list first, folding into stack
/// scratch. Every one of those is a way to get the answer wrong quietly — an intersection that
/// starts from the wrong list, a merge that mis-steps, a fold that changes a char — so the cover
/// here is an ORACLE: the same question answered by brute force over the same input.</para>
/// </summary>
public sealed class SegmentTrigramLookupTests
{
    private static SegmentTrigramIndex Build(params string[] messages)
    {
        var idx = new SegmentTrigramIndex();
        for (int i = 0; i < messages.Length; i++) idx.Add((uint)i, messages[i]);
        return idx;
    }

    /// <summary>What the index is allowed to return: every row that really contains the term,
    /// plus any row sharing all its trigrams (the index is a filter, not a matcher).</summary>
    private static uint[] TrigramOracle(string[] messages, string term)
    {
        var lower = term.ToLowerInvariant();
        var keys  = new List<(char, char, char)>();
        for (int i = 0; i <= lower.Length - 3; i++) keys.Add((lower[i], lower[i + 1], lower[i + 2]));

        var hits = new List<uint>();
        for (int m = 0; m < messages.Length; m++)
        {
            var text = messages[m].ToLowerInvariant();
            bool all = true;
            foreach (var (a, b, c) in keys)
            {
                bool found = false;
                for (int i = 0; i + 2 < text.Length && !found; i++)
                    found = text[i] == a && text[i + 1] == b && text[i + 2] == c;
                if (!found) { all = false; break; }
            }
            if (all) hits.Add((uint)m);
        }
        return [.. hits];
    }

    private static void AssertAscendingDistinct(uint[] offsets)
    {
        for (int i = 1; i < offsets.Length; i++)
            Assert.True(offsets[i] > offsets[i - 1],
                $"offsets are not ascending and distinct at {i}: {offsets[i - 1]} then {offsets[i]}");
    }

    /// <summary>
    /// The oracle, over both phases of the index — the build-phase lists and the deserialised
    /// arrays take different branches into the same merge.
    /// </summary>
    [Fact]
    public void RandomisedTermsMatchBruteForce()
    {
        var rng   = new Random(20260914);
        var words = new[]
        {
            "timeout", "order", "settled", "payment", "refused", "gateway", "retry",
            "платёж", "проведён", "cache", "miss", "connection", "reset", "amount",
        };

        var messages = new string[400];
        for (int i = 0; i < messages.Length; i++)
        {
            int n = 2 + rng.Next(5);
            var parts = new string[n];
            for (int k = 0; k < n; k++) parts[k] = words[rng.Next(words.Length)];
            // Mixed case on the stored side too: folding happens on both.
            messages[i] = (i % 3 == 0 ? string.Join(' ', parts).ToUpperInvariant() : string.Join(' ', parts))
                        + " #" + i;
        }

        var built  = Build(messages);
        var loaded = SegmentTrigramIndex.Deserialise(built.Serialise());

        foreach (var term in new[]
                 {
                     "timeout", "TIMEOUT", "TimeOut", "order settled", "payment refused",
                     "gateway", "платёж", "ПЛАТЁЖ", "connection reset", "#137",
                     "zzz not here", "ret", "y r", "amountamount",
                 })
        {
            var expected = TrigramOracle(messages, term);

            foreach (var (name, idx) in new[] { ("build", built), ("loaded", loaded) })
            {
                var got = idx.Lookup(term);
                Assert.NotNull(got);
                AssertAscendingDistinct(got!);
                Assert.Equal(expected, got);
                Assert.True(got.Length >= 0, name);
            }
        }
    }

    /// <summary>
    /// Rarest list first must not change the answer. Here one trigram names nearly every row and
    /// another names one, so the merge starts from the opposite end of the term than it used to.
    /// </summary>
    [Fact]
    public void StartingFromTheRarestTrigramGivesTheSameAnswer()
    {
        var messages = new string[300];
        for (int i = 0; i < messages.Length; i++) messages[i] = "common text here " + i;
        messages[177] = "common text here xyzzy 177";

        var idx = SegmentTrigramIndex.Deserialise(Build(messages).Serialise());

        // "xyzzy" is rare, "common" is everywhere; the term needs both.
        Assert.Equal([177u], idx.Lookup("here xyzzy")!);
        Assert.Equal(TrigramOracle(messages, "common text"), idx.Lookup("common text")!);
    }

    /// <summary>
    /// An out-of-order build is a contract violation the index RECORDS rather than rejects, and a
    /// sorted merge over unordered lists would silently drop rows. Lookup must still be right.
    /// </summary>
    [Fact]
    public void OffsetsThatArriveOutOfOrderStillAnswerCorrectly()
    {
        var idx = new SegmentTrigramIndex();
        idx.Add(9, "gateway timeout");
        idx.Add(3, "gateway timeout");     // below the tail → marks the index unsorted
        idx.Add(7, "gateway refused");

        var got = idx.Lookup("timeout");
        Assert.NotNull(got);
        AssertAscendingDistinct(got!);
        Assert.Equal([3u, 9u], got);

        Assert.Equal([3u, 7u, 9u], idx.Lookup("gateway")!);
        Assert.Empty(idx.Lookup("refund")!);

        // And serialisation repairs the order, so the loaded index agrees.
        var loaded = SegmentTrigramIndex.Deserialise(idx.Serialise());
        Assert.Equal([3u, 9u], loaded.Lookup("timeout")!);
        Assert.Equal([3u, 7u, 9u], loaded.Lookup("gateway")!);
    }

    /// <summary>A term past the stack-scratch threshold takes the pooled buffer.</summary>
    [Fact]
    public void ATermLongerThanTheStackScratchIsFoldedInAPooledBuffer()
    {
        string long1 = new string('q', 2000) + "NEEDLE" + new string('q', 2000);
        var idx = SegmentTrigramIndex.Deserialise(Build(long1, "something else entirely").Serialise());

        Assert.Equal([0u], idx.Lookup(long1.ToUpperInvariant())!);
        Assert.Equal([0u], idx.Lookup("needle")!);
        Assert.Empty(idx.Lookup(new string('q', 1500) + "haystack")!);
    }

    /// <summary>
    /// The three answers Lookup has to keep separate: "no information" (null), "provably
    /// nothing" (empty) and a hit. Conflating the first two is what made substring search over
    /// non-ASCII return no rows at all before the V2 key.
    /// </summary>
    [Fact]
    public void NullAndEmptyKeepTheirDifferentMeanings()
    {
        var populated = SegmentTrigramIndex.Deserialise(Build("gateway timeout").Serialise());

        Assert.Null(populated.Lookup("ab"));               // shorter than a trigram → no info
        Assert.Null(populated.Lookup(""));                 // ditto
        Assert.Empty(populated.Lookup("zzzzz")!);          // populated index, absent trigram
        Assert.Equal([0u], populated.Lookup("timeout")!);

        var empty = new SegmentTrigramIndex();
        Assert.Null(empty.Lookup("timeout"));              // never built → no info, scan
    }
}
