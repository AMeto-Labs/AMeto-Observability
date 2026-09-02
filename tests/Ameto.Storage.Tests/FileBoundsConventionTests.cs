using System.Text.RegularExpressions;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE RULE, ENFORCED INSTEAD OF RESTATED.
///
/// <para>Four review rounds ran the same way: a reader was found sizing an allocation by a number
/// it had just read out of a file, the reported one was bounded, a comment was written explaining
/// the rule — and the identical shape one file over survived to be reported next round. The
/// <c>.stats</c> count was fixed with a measurement in its comment while <c>ServiceGraphSidecar</c>
/// measured the same four gigabytes untouched; then every COUNT was bounded while every block
/// LENGTH stayed on a constant, which one flipped byte turned into 64 MB rented out of a 3.5 KB
/// file. Prose in one method's comment is not a rule — this is.</para>
///
/// <para>It reads the sources rather than the IL because the property is syntactic: a size argument
/// that is not a literal has to have a bound in view. That makes it blunt, so anything it cannot
/// see through is listed in <see cref="Exempt"/> with the reason written down — a list that is
/// meant to be argued with, not extended quietly.</para>
/// </summary>
public sealed class FileBoundsConventionTests
{
    private readonly ITestOutputHelper _out;
    public FileBoundsConventionTests(ITestOutputHelper output) => _out = output;

    /// <summary>Sites the scan flags that are sound for a reason it cannot see. Each needs a why.</summary>
    private static readonly Dictionary<string, string> Exempt = new()
    {
        ["SpanWriter.cs"]         = "writes files rather than reading them; every size is our own",
        ["MetricWriteAheadLog.cs"] = "not a trace reader",
        ["SpanStats.cs"]           = "a static table of bucket bounds; no file involved",
    };

    /// <summary>
    /// Individual lines the scan cannot see the bound for, each with the reason it is sound. Adding
    /// to this list is a claim, and the claim is written down so the next reviewer can dispute it.
    /// </summary>
    private static readonly Dictionary<string, string> ExemptSite = new()
    {
        ["SpanBloom.cs:var bitset = new byte[bits / 8]"] =
            "bits is the filter geometry this process chose, not a number read back from a file",
        ["SpanReader.cs:var arr = new byte[(int)totalLen]"] =
            "ReadBytesFixed refuses totalLen over 1 KB three lines above",
        ["SpanWriteAheadLog.cs:attrs = new byte[eh.AttrLength]"] =
            "AttrLength is bounded by the WAL entry header check in ReadEntry",
        ["TraceStorageEngine.cs:var combined = new List<SpanRecord>(snapshot.Count + _hotSpans.Count)"] =
            "both counts are in-memory collection sizes",
        ["TraceStorageEngine.cs:var known = new HashSet<string>(_coldSegments.Select(s => s.FilePath), StringComparer.Ordinal)"] =
            "sized from the in-memory segment snapshot",
        ["TraceStorageEngine.cs:var next  = new List<SpanSegmentInfo>(loaded.Count + _coldSegments.Length)"] =
            "both counts are in-memory collection sizes",
        ["TraceStorageEngine.cs:var nodeNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)"] =
            "a comparer, not a capacity",
        ["TraceStorageEngine.cs:var total = new double[buckets]"] =
            "buckets is the caller's requested resolution, clamped by the endpoint",
        ["TraceStorageEngine.cs:var error = new double[buckets]"] =
            "buckets is the caller's requested resolution, clamped by the endpoint",
        ["TraceSummarySidecar.cs:(a.Services ??= new HashSet<string>(2, StringComparer.Ordinal)).Add(s.ServiceName)"] =
            "a literal capacity of two",
    };

    /// <summary>What "a bound is in view" looks like.</summary>
    private static readonly string[] BoundMarkers =
    [
        "FileBounds.", "MaxBlockBytes", "MergeBlockBytes", "MaxBodyBytes", "MaxListPrealloc",
        "MaxTotalBytes", "MaxRegions", "MaxPaths",
    ];

    /// <summary>Allocation shapes whose size argument is worth checking.</summary>
    private static readonly Regex Alloc = new(
        @"(ArrayPool<\w+>\.Shared\.Rent\(|new List<[^>]+>\(|new HashSet<[^>]+>\(|new \w+\[|\.ReadBytes\()",
        RegexOptions.Compiled);

    /// <summary>
    /// Sizes that cannot come from a file: a literal, a named constant, a length taken from
    /// something already in memory, or a 16-bit read (a ushort cannot ask for more than 64 KB,
    /// which is not the failure this rule is about).
    /// </summary>
    /// <summary>An allocation with no size argument at all — new List&lt;T&gt;(), r.ReadBytes().</summary>
    private static readonly Regex EmptyArgs = new(@"(\(\s*\)|\[\s*\])", RegexOptions.Compiled);

    private static readonly Regex SafeSize = new(
        @"(Rent|new \w+|List<[^>]+>|HashSet<[^>]+>|ReadBytes)\s*[\(\[]\s*\(?\s*(int\)\s*)?("
      + @"\d[\d_]*"                     // a literal
      + @"|[A-Za-z_]\w*\.(Count|Length)" // the size of something already in memory
      + @"|Max[A-Z]\w*"                  // one of the named ceilings
      + @"|nameLen|expectedLen|len\b"    // 16-bit lengths
      + @")\s*[\)\]]",
        RegexOptions.Compiled);

    [Fact]
    public void Every_file_sized_allocation_in_the_storage_readers_has_a_bound_in_view()
    {
        string dir = StorageSourceDir();
        var offenders = new List<string>();
        int scannedFiles = 0, sizedSites = 0;

        foreach (string file in Directory.EnumerateFiles(dir, "*.cs"))
        {
            string name = Path.GetFileName(file);
            if (Exempt.ContainsKey(name)) continue;
            scannedFiles++;

            var lines = File.ReadAllLines(file);
            for (int i = 0; i < lines.Length; i++)
            {
                string line = lines[i];
                if (line.TrimStart().StartsWith("//")) continue;
                if (!Alloc.IsMatch(line)) continue;
                // No size argument at all — new List<T>(), r.ReadBytes() — is not a sized
                // allocation and has nothing to bound.
                if (EmptyArgs.IsMatch(line)) continue;
                if (SafeSize.IsMatch(line)) continue;
                sizedSites++;

                // A bound within the twenty-five lines above: the same method in practice, and
                // close enough that a reader meets it before the allocation.
                bool bounded = false;
                for (int k = Math.Max(0, i - 25); k <= i && !bounded; k++)
                    bounded = BoundMarkers.Any(mk => lines[k].Contains(mk, StringComparison.Ordinal));

                if (bounded) continue;
                if (ExemptSite.ContainsKey($"{name}:{line.Trim().TrimEnd(';')}")) continue;
                offenders.Add($"{name}:{i + 1}  {line.Trim()}");
            }
        }

        foreach (string o in offenders) _out.WriteLine(o);

        // A SCAN THAT MATCHES NOTHING PASSES EVERYTHING, which is the decorative-test shape this
        // change keeps running into. Assert the instrument works before believing its result: an
        // earlier version of this regex recognised zero sites and stayed green with a bound
        // deleted from a reader.
        _out.WriteLine($"scanned {scannedFiles} file(s), {sizedSites} unguarded-shaped allocation(s)");
        Assert.True(scannedFiles >= 5, $"the scan only found {scannedFiles} source files");
        Assert.True(sizedSites >= 8,
            $"the scan recognised only {sizedSites} allocations sized by something non-constant — it "
          + "has stopped seeing the shape it exists to police, so its green result means nothing");

        Assert.True(offenders.Count == 0,
            $"{offenders.Count} allocation(s) in the storage readers are sized by something that may "
          + "have come out of a file, with no bound in the twenty-five lines above them. Bound it "
          + "with FileBounds, or add it to Exempt WITH A REASON:" + Environment.NewLine
          + string.Join(Environment.NewLine, offenders));
    }
    [Fact]
    public void The_bound_uses_the_larger_of_the_two_element_sizes()
    {
        // The asymmetry that made "count <= bytes / 4" permit four times the file: a HashSet<uint>
        // stores four bytes per element on disk and costs about sixteen once built.
        Assert.Equal(100, FileBounds_MaxCountThatFits(1600, 4, 16));
        Assert.Equal(100, FileBounds_MaxCountThatFits(1600, 16, 4));
        Assert.Equal(400, FileBounds_MaxCountThatFits(1600, 4, 4));
        Assert.Equal(0,   FileBounds_MaxCountThatFits(0, 4, 16));
        Assert.Equal(0,   FileBounds_MaxCountThatFits(-1, 4, 16));
    }

    /// <summary>Reaches the internal helper the same way the readers do.</summary>
    private static long FileBounds_MaxCountThatFits(long bytesRemaining, int fileBytes, int heapBytes)
    {
        var t = typeof(Ameto.Tracing.Storage.SpanReader).Assembly
            .GetType("Ameto.Tracing.Storage.FileBounds", throwOnError: true)!;
        var m = t.GetMethod("MaxCountThatFits",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        return (long)m.Invoke(null, [bytesRemaining, fileBytes, heapBytes])!;
    }

    private static string StorageSourceDir()
    {
        var d = new DirectoryInfo(AppContext.BaseDirectory);
        while (d is not null && !Directory.Exists(Path.Combine(d.FullName, "src", "Ameto.Tracing", "Storage")))
            d = d.Parent;
        Assert.NotNull(d);
        return Path.Combine(d!.FullName, "src", "Ameto.Tracing", "Storage");
    }
}
