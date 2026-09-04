using System.Text.RegularExpressions;
using Xunit.Abstractions;

namespace Ameto.Storage.Tests;

/// <summary>
/// THE RULE, ENFORCED INSTEAD OF RESTATED.
///
/// <para>Round after round ran the same way: a reader was found sizing an allocation by a number it
/// had just read out of a file, the reported one was bounded, a comment was written explaining the
/// rule — and the identical shape one file over survived to be reported next round. The
/// <c>.stats</c> count was fixed with a measurement in its comment while <c>ServiceGraphSidecar</c>
/// measured the same four gigabytes untouched; then every COUNT was bounded while every block
/// LENGTH stayed on a constant, which one flipped byte turned into 64 MB rented out of a 3.5 KB
/// file. Prose in one method's comment is not a rule — this is.</para>
///
/// <para>WHAT THIS CAN AND CANNOT PROVE, because the first version of it certified the wrong thing.
/// It flagged an allocation only when the whole line failed a blunt pattern, so five of six
/// plausible ways to write an unbounded one walked past: an inline <c>new uint[br.ReadUInt32()]</c>
/// read as "no size argument at all"; a 32-bit length called <c>len</c> matched an exemption meant
/// for 16-bit ones; and any <c>FileBounds</c> call anywhere in the preceding lines certified an
/// unrelated allocation, including one that appeared inside a COMMENT. Two of that round's own
/// fixes could be deleted with the suite still green.</para>
///
/// <para>So the bound now has to name the same thing the allocation is sized by, comments do not
/// count as code, and an allocation whose size is read inline is unbounded by definition. What is
/// still beyond a textual scan is an allocation sized by a value laundered through arithmetic or a
/// helper call — this is a floor, not a proof, and <see cref="ExemptSite"/> is where the claims it
/// cannot check are written down to be argued with.</para>
/// </summary>
public sealed class FileBoundsConventionTests
{
    private readonly ITestOutputHelper _out;
    public FileBoundsConventionTests(ITestOutputHelper output) => _out = output;

    /// <summary>
    /// Every directory of readers this rule covers. Metrics is here because it was NOT: the scan
    /// used to stop at the tracing folder while <c>MetricReader</c> rented four buffers straight
    /// from an untrusted <c>.mts</c> header, on the same 512 MB box — and the exemption list named
    /// a metrics file the scan could not even reach, which reads as "checked and excluded".
    /// </summary>
    private static readonly string[] ScannedDirs =
    [
        Path.Combine("src", "Ameto.Tracing", "Storage"),
        Path.Combine("src", "Ameto.Metrics", "Storage"),
    ];

    /// <summary>Whole files the rule does not apply to. Each needs a why.</summary>
    private static readonly Dictionary<string, string> Exempt = new()
    {
        ["SpanWriter.cs"]  = "writes files rather than reading them; every size is our own",
        ["SpanStats.cs"]   = "a static table of bucket bounds; no file involved",
        ["MetricWriter.cs"] = "writes files rather than reading them",
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
        ["SpanReader.cs:var dict = new Dictionary<string, object?>(cnt, StringComparer.Ordinal)"] =
            "cnt is a MessagePack map header inside a block already bounded by MaxBlockBytes",
        ["TraceStorageEngine.cs:var merged = new Dictionary<TraceId, MergedTrace>(scanCap)"] =
            "scanCap is derived from the caller's limit, not from any file",
        ["TraceStorageEngine.cs:var total = new double[buckets]"] =
            "buckets is the resolution the caller asked for; the API clamps it to 1..1000",
        ["TraceStorageEngine.cs:var error = new double[buckets]"] =
            "buckets is the resolution the caller asked for; the API clamps it to 1..1000",
        ["SpanReader.cs:if (seq is null) return new byte[expectedLen]"] =
            "expectedLen is a call-site constant (8 or 16, an id width), never a file field",
        ["MetricStorageEngine.cs:public ExemplarRing(int capacity) => _buf = new ExemplarSample[capacity]"] =
            "the ring is a hot-tier structure sized by config; nothing reads it back off disk",
        ["MetricStorageEngine.cs:var outArr = new ExemplarSample[_count]"] =
            "_count is the ring's own occupancy, bounded by the capacity above",
        ["MetricWriteAheadLog.cs:var snapshot = new byte[kept]"] =
            "kept is Math.Min(orphaned, 4 MiB) on the line above — already clamped by a literal",
        ["MetricWriteAheadLog.cs:buckets = new long[eh.BucketCount]"] =
            "the entry header check above rejects BucketCount that runs past the mapped end",
        ["MetricWriteAheadLog.cs:var body = new byte[len]"] =
            "len is refused above 8 MiB two lines above, and one record is read at a time",
        ["MetricStorageEngine.cs:: new HashSet<SeriesKey>(keys.GetRange(off, take))"] =
            "a slice of an in-memory list",
        // Found the moment the scan learned to read nested generics — the SECOND site that shape
        // was hiding, after MetricReader.ReadLabels, which is the reason the blindness mattered.
        // This one turns out to be sound; the point is that nobody could have said so before.
        ["MetricStorageEngine.cs:var acc = new Dictionary<SeriesKey, List<MetricDataPoint>>(take)"] =
            "take is Math.Min(SeriesChunk, keys.Count - off) on the line above — the chunk size "
          + "this process chose, over an in-memory key list; the HashSet exempted just above is "
          + "the same `take`",
        ["MetricStorageEngine.cs:var copy = new List<MetricDataPoint>(_points)"] =
            "a copy of an in-memory list",
        // COPY CONSTRUCTORS, NOT CAPACITIES. `new List<T>(someCollection)` and
        // `new List<T>(someCount)` are one shape to a textual scan and opposites in fact — the
        // first copies what is already in memory, the second reserves for a number a file may have
        // supplied, which is the only one this rule is about. The manifest rebuilds its whole state
        // on every mutation (immutable snapshots, swapped under a lock), so it does this a lot; the
        // three helpers below are the ONLY places it does, deliberately, so this list stays three
        // arguable claims instead of seven unarguable ones. Sizes that really do come out of the
        // manifest file go through Cursor.Count → FileBounds.RequireCountFits.
        ["TraceManifest.cs:public Dictionary<ulong, TraceSegmentEntry> CopySegments() => new(Segments)"] =
            "copies the segment map already in memory; not a capacity",
        ["TraceManifest.cs:public List<TraceIndexRun>                  CopyRuns()     => new(Runs)"] =
            "copies the run list already in memory; not a capacity",
        ["TraceManifest.cs:public HashSet<ulong>                       CopyCovered()  => new(Covered)"] =
            "copies the coverage set already in memory; not a capacity",
        ["TraceManifest.cs:var drop = new HashSet<string>(removedPaths, StringComparer.Ordinal)"] =
            "copies the caller's own collection; not a capacity",
    };

    /// <summary>What "a bound is in view" looks like.</summary>
    /// <summary>The helper that throws; the rest of <see cref="BoundMarkers"/> only return.</summary>
    private const string ThrowingGuard = "FileBounds.Require";

    private static readonly string[] BoundMarkers =
    [
        "FileBounds.", "MaxBlockBytes", "MergeBlockBytes", "MaxBodyBytes", "MaxListPrealloc",
        "MaxTotalBytes",
    ];

    /// <summary>
    /// The HEAD of an allocation — up to but not including the bracket its size sits in. Both the
    /// generic argument list and the size argument are then read with counters, not patterns.
    ///
    /// <para>THE GENERIC LIST USED TO BE PART OF THIS PATTERN, AS <c>&lt;[^&gt;]*&gt;</c>, AND
    /// THAT IS A HOLE WITH A NAME. <c>[^&gt;]*</c> stops at the first <c>&gt;</c>: given
    /// <c>new List&lt;KeyValuePair&lt;string, string&gt;&gt;(count)</c> it consumed
    /// <c>KeyValuePair&lt;string, string</c>, matched the first <c>&gt;</c>, then demanded a
    /// <c>(</c> and found the second <c>&gt;</c> — so an allocation whose type argument is itself
    /// generic did not match AT ALL. Not a corner case: it is the exact shape of
    /// <c>MetricReader.ReadLabels</c>, whose capacity came straight off a msgpack map header, in a
    /// directory <see cref="ScannedDirs"/> walks. The file was read, the shape was in it, and this
    /// scan reported nothing — for four rounds, while its own doc comment claimed the coverage.
    /// The lesson is the one already written above <see cref="BracketedArgument"/>: nesting is not
    /// a regular language, and every attempt to spell it as one has missed a real allocation.</para>
    /// </summary>
    private static readonly Regex AllocHead = new(
        @"\bArrayPool<\w+>\.Shared\.Rent(?=\s*\()"
      + @"|\.ReadBytes(?=\s*\()"
      + @"|\bnew\s+(?:\w+\.)*(?<type>\w+)",
        RegexOptions.Compiled);

    /// <summary>
    /// Types whose CONSTRUCTOR allocates nothing — a view over memory that already exists, or a
    /// value type built from its parts.
    ///
    /// <para>Consulted at the constructor case only, never in the pattern, which is where it used
    /// to live. <c>new Span&lt;byte&gt;(ptr, 16)</c> is a view; <c>new KeyValuePair&lt;string,
    /// string&gt;[n]</c> is still an array of n, and excluding the type outright would have waved
    /// it through.</para>
    /// </summary>
    private static readonly HashSet<string> ViewNotAllocation = new(StringComparer.Ordinal)
    {
        "Span", "ReadOnlySpan", "Memory", "ReadOnlyMemory", "ArraySegment",
        "KeyValuePair", "MessagePackReader", "SequenceReader", "Nullable",
    };

    /// <summary>A size read straight out of the file on the same line — unbounded by construction.</summary>
    private static readonly Regex InlineRead = new(
        @"\b(Read(UInt|Int)(32|64)|BinaryPrimitives\.Read\w+)\s*\(", RegexOptions.Compiled);

    /// <summary>
    /// A size nothing in a file can influence: a literal, a named ceiling, or arithmetic over the
    /// sizes of things already in memory.
    /// </summary>
    private static readonly Regex ConstantSize = new(
        @"^[\d_\s\+\-\*/\(\)]*$"                                  // pure arithmetic on literals
      + @"|^\s*Max[A-Z]\w*\s*$"                                    // a named ceiling
      + @"|^[^\""]*\.(Count|Length)\b[^\""]*$"                       // sized from memory
      + @"|^\s*\w+Count(Locked)?\(\)\s*$"                          // …or a count method
      + @"|^\s*\d[\d_]*\s*,"                                        // a literal capacity, then a comparer
      + @"|^\s*\w*Comparer\.\w+\s*$"                              // a comparer, not a capacity
      + @"|\.Select\(|=>"                                          // projected from memory
      + @"|Math\.(Min|Clamp)\([^)]*\d[\d_]*\s*\)",                 // already clamped by a literal
        RegexOptions.Compiled);

    /// <summary>
    /// A read that cannot ask for much: 16 bits is 64 KB and 8 bits is 255. The rule is about a
    /// 32-bit field turning a small file into a gigabyte request, and these cannot.
    /// </summary>
    private static readonly Regex NarrowRead = new(
        @"\bRead(UInt16|Int16|Byte)\s*\(", RegexOptions.Compiled);

    /// <summary>A local declared as a 16- or 8-bit read: it cannot ask for more than 64 KB.</summary>
    private static readonly Regex NarrowLocal = new(
        @"\b(?:ushort|byte)\s+(\w+)\s*="
      + @"|\b(?:int|uint|long|ulong|var)\s+(\w+)\s*=[^;]*\bRead(?:UInt16|Int16|Byte)\s*\(",
        RegexOptions.Compiled);

    [Fact]
    public void Every_file_sized_allocation_in_the_readers_names_its_bound()
    {
        var offenders = new List<string>();
        int scannedFiles = 0, sizedSites = 0;

        foreach (string rel in ScannedDirs)
        {
            string dir = Path.Combine(RepoRoot(), rel);
            Assert.True(Directory.Exists(dir), $"the scan cannot reach {rel}");

            foreach (string file in Directory.EnumerateFiles(dir, "*.cs"))
            {
                string name = Path.GetFileName(file);
                if (Exempt.ContainsKey(name)) continue;
                scannedFiles++;

                sizedSites += ScanLines(name, File.ReadAllLines(file), offenders);
            }
        }

        foreach (string o in offenders) _out.WriteLine(o);

        // A SCAN THAT MATCHES NOTHING PASSES EVERYTHING, which is the decorative-test shape this
        // change keeps running into. Assert the instrument works before believing its result: an
        // earlier version recognised zero sites and stayed green with a bound deleted from a reader.
        _out.WriteLine($"scanned {scannedFiles} file(s), {sizedSites} file-sized allocation(s)");
        Assert.True(scannedFiles >= 8, $"the scan only found {scannedFiles} source files");
        Assert.True(sizedSites >= 8,
            $"the scan recognised only {sizedSites} allocations sized by something non-constant — it "
          + "has stopped seeing the shape it exists to police, so its green result means nothing");

        Assert.True(offenders.Count == 0,
            $"{offenders.Count} allocation(s) are sized by something that may have come out of a file, "
          + "with no bound naming it. Bound it with FileBounds, or add it to ExemptSite WITH A "
          + "REASON:" + Environment.NewLine + string.Join(Environment.NewLine, offenders));
    }

    /// <summary>
    /// Every allocation on this line, given as the index of the bracket its size sits in — a
    /// <c>(</c> for a constructor or a <c>Rent</c>, a <c>[</c> for an array.
    /// </summary>
    private static IEnumerable<int> AllocationSizeBrackets(string line)
    {
        foreach (Match m in AllocHead.Matches(line))
        {
            int  at      = SkipSpace(line, m.Index + m.Length);
            bool generic = false;

            // The generic argument list, COUNTED. This is the step the old pattern could not take.
            if (at < line.Length && line[at] == '<')
            {
                int close = MatchingAngle(line, at);
                if (close < 0) continue;            // not a type list, or it wraps — say nothing
                at      = SkipSpace(line, close + 1);
                generic = true;
            }

            if (at >= line.Length) continue;
            if (line[at] == '[') { yield return at; continue; }   // an array is an allocation, always
            if (line[at] != '(') continue;                        // `new Foo { … }`, a bare `new Foo`, …

            var type = m.Groups["type"];
            if (!type.Success) { yield return at; continue; }     // a Rent or a ReadBytes

            // A CONSTRUCTOR COUNTS ONLY WHEN IT IS GENERIC, which is the one restriction the old
            // pattern got right and worth keeping deliberately rather than by accident: the
            // collections that take a capacity — List<T>, Dictionary<K,V>, HashSet<T> — are
            // generic, and everything else with a first argument is a `new BinaryReader(fs)` or a
            // `new InvalidDataException($"…")`. Dropping this turned 102 constructors into
            // "allocations sized by something from a file" and buried the two that were.
            if (!generic) continue;
            if (ViewNotAllocation.Contains(type.Value)) continue;
            yield return at;
        }
    }

    private static int SkipSpace(string s, int i)
    {
        while (i < s.Length && char.IsWhiteSpace(s[i])) i++;
        return i;
    }

    /// <summary>
    /// The <c>&gt;</c> closing the <c>&lt;</c> at <paramref name="open"/>, or -1 when there is
    /// none on this line. Bails out on a character no type argument list contains, so a stray
    /// <c>&lt;</c> cannot make the walk run to the end of the line and report nonsense.
    /// </summary>
    private static int MatchingAngle(string s, int open)
    {
        int depth = 0;
        for (int k = open; k < s.Length; k++)
        {
            char c = s[k];
            if (c == '<') depth++;
            else if (c == '>') { if (--depth == 0) return k; }
            else if (c is '(' or ')' or ';' or '{' or '}' or '=') return -1;
        }
        return -1;
    }

    /// <summary>The text between a bracket at <paramref name="open"/> and its match.</summary>
    private static string? BracketedArgument(string line, int open)
    {
        char closer = line[open] == '(' ? ')' : ']';
        int depth = 0;
        for (int k = open; k < line.Length; k++)
        {
            if (line[k] == '(' || line[k] == '[') depth++;
            else if (line[k] == ')' || line[k] == ']')
            {
                depth--;
                if (depth == 0) return line[k] == closer ? line[(open + 1)..k] : null;
            }
        }
        return null;
    }
    [Fact]
    public void The_count_bound_is_the_files_own_bytes_and_nothing_else()
    {
        // The arithmetic that refused healthy segments. bytesRemaining counts bytes ON DISK, so a
        // legitimate count of N occupies N * fileBytesPerElement — mixing an in-memory size into
        // the divisor made the limit tighter than the format allows, four times over for a
        // HashSet<uint>, and threw InvalidDataException over intact 20 000- and 50 000-span files.
        Assert.Equal(400, MaxCountThatFits(1600, 4));
        Assert.Equal(100, MaxCountThatFits(1600, 16));
        Assert.Equal(0,   MaxCountThatFits(0, 4));
        Assert.Equal(0,   MaxCountThatFits(-1, 4));

        // What an entry costs once BUILT caps the RESERVATION instead, without refusing the read.
        Assert.Equal(1000, PreallocFor(1000, 16));                       // modest: reserve it all
        Assert.True(PreallocFor(100_000_000, 16) < 1_000_000,            // vast: reserve a ceiling
            "a huge but legitimate count still reserved for every entry");
        Assert.Equal(0, PreallocFor(-5, 16));
    }

    private static bool IsComment(string line)
    {
        string t = line.TrimStart();
        return t.StartsWith("//") || t.StartsWith("*") || t.StartsWith("/*");
    }

    /// <summary>The leading identifier of a size expression, past any cast.</summary>
    private static string? SizeIdentifier(string arg)
    {
        var m = Regex.Match(arg, @"^\s*(?:\(\s*\w+\s*\)\s*)?([A-Za-z_]\w*)");
        return m.Success ? m.Groups[1].Value : null;
    }

    /// <summary>A bound in the preceding lines that MENTIONS the thing being sized.</summary>
    /// <summary>
    /// The bound must name what it bounds, INSIDE ITS OWN ARGUMENTS. Anywhere on the line was not
    /// enough: a check over unrelated numbers sitting on the same line as the declaration it does
    /// not cover — <c>uint n = br.ReadUInt32(); FileBounds.RequireCountFits(1, 8, 4, …);</c> —
    /// mentioned the identifier by pure adjacency and certified an allocation nothing had checked.
    /// </summary>

    /// <summary>
    /// The scan itself, over one file's lines. It is a method and not an inlined loop so that
    /// <see cref="The_scan_still_sees_the_shapes_that_once_walked_past_it"/> can put the known
    /// bypasses through THE SAME code the readers are checked with — a scanner verified by hand
    /// once is a scanner that quietly stops matching on the next edit.
    /// </summary>
    private static int ScanLines(string name, string[] lines, List<string> offenders)
    {
        int sizedSites = 0;
        var narrow = new HashSet<string>(StringComparer.Ordinal);

        for (int i = 0; i < lines.Length; i++)
        {
            string line = lines[i];
            if (IsComment(line)) continue;

            foreach (Match nm in NarrowLocal.Matches(line))
                narrow.Add(nm.Groups[1].Success ? nm.Groups[1].Value : nm.Groups[2].Value);

            foreach (int open in AllocationSizeBrackets(line))
            {
                string? arg = BracketedArgument(line, open);
                if (arg is null) continue;
                arg = arg.Trim();
                if (arg.Length == 0) continue;              // no size argument at all
                if (ConstantSize.IsMatch(arg)) continue;    // nothing from a file

                // The reservation IS the helper's answer: PreallocFor caps it by construction,
                // whatever the file claimed.
                if (arg.Contains("FileBounds.PreallocFor", StringComparison.Ordinal)) continue;

                string? sized = SizeIdentifier(arg);
                if (sized is not null && narrow.Contains(sized)) continue;   // 16-bit at most
                if (NarrowRead.IsMatch(arg) && !InlineRead.IsMatch(arg)) continue;

                sizedSites++;

                // A size read on this very line has no bound anywhere, by construction.
                bool inline = InlineRead.IsMatch(arg);

                // Otherwise the bound has to NAME what the allocation is sized by. Any FileBounds
                // call in the vicinity used to do, which certified allocations that had nothing
                // to do with it.
                bool bounded = !inline && sized is not null && NamedBoundAbove(lines, i, sized);

                if (bounded) continue;
                if (ExemptSite.ContainsKey($"{name}:{line.Trim().TrimEnd(';')}")) continue;
                offenders.Add($"{name}:{i + 1}  sized by '{(inline ? "an inline read" : sized ?? arg)}'  {line.Trim()}");
            }
        }
        return sizedSites;
    }

    /// <summary>
    /// The shapes an unbounded allocation actually takes in this codebase. Most of them used to
    /// walk straight through: the scan could not see array allocations AT ALL (the pattern
    /// consumed the bracket and then demanded a second one), a qualified type name hid a List, a
    /// bound was accepted for merely sharing a line with the identifier it never covered — and, up
    /// to this round, a NESTED GENERIC was invisible, which is how the metric reader's
    /// <c>List&lt;KeyValuePair&lt;string, string&gt;&gt;(count)</c> sat inside a scanned directory
    /// being reported by nobody. Each entry is a line the scan MUST report; the paired negatives
    /// are lines it must not.
    /// </summary>
    [Fact]
    public void The_scan_still_sees_the_shapes_that_once_walked_past_it()
    {
        string[] mustFlag =
        [
            "        var a = new uint[br.ReadUInt32()];",
            "        var b = ArrayPool<byte>.Shared.Rent((int)br.ReadUInt32());",
            "        uint len = br.ReadUInt32();\n        var c = ArrayPool<byte>.Shared.Rent((int)len);",
            "        uint n = br.ReadUInt32(); FileBounds.RequireCountFits(1, 8, 4, \"x\", \"y\");\n"
          + "        var d = new System.Collections.Generic.List<uint>((int)n);",
            "        uint cnt = br.ReadUInt32();\n        var e = new string[cnt];",
            "        int m = br.ReadInt32();\n        var f = new Dictionary<string, int>(m);",
            // The nested generic. Exactly MetricReader.ReadLabels as it stood, and invisible to
            // every version of this scan before the angle brackets were counted.
            "        int count = r.ReadMapHeader();\n"
          + "        var pairs = new List<KeyValuePair<string, string>>(count);",
            // Two levels down, and qualified: the counter has to hold for both.
            "        int c2 = r.ReadMapHeader();\n"
          + "        var deep = new System.Collections.Generic.List<Dictionary<string, List<int>>>(c2);",
            // An ARRAY of a type the constructor exclusions name. `new KeyValuePair<..>(k, v)`
            // allocates nothing; `new KeyValuePair<..>[n]` allocates n of them, and folding the
            // exclusion into the pattern (where it used to live) waved this through.
            "        uint kn = br.ReadUInt32();\n        var kvs = new KeyValuePair<string, string>[kn];",
        ];

        string[] mustNotFlag =
        [
            "        var g = new byte[16];",
            "        ushort small = br.ReadUInt16();\n        var h = new byte[small];",
            "        int narrow = r.ReadUInt16();\n        var i2 = new long[narrow];",
            "        uint k = br.ReadUInt32();\n"
          + "        FileBounds.RequireCountFits(k, fs.Length - fs.Position, 4, \"x\", \"y\");\n"
          + "        var j = new uint[k];",
            "        uint q = br.ReadUInt32();\n"
          + "        if (q > FileBounds.MaxCountThatFits(fs.Length - fs.Position, 6)) return [];\n"
          + "        var l = new string[q];",
            "        var m2 = new double[FileBounds.PreallocFor(cnt, heapBytesPerElement: 8)];",
            "        var n2 = new Span<byte>(ptr, 16);",
            // The nested generic, bounded — the fix for the flagged shape above must be accepted,
            // or the scan has only moved the problem from silence to noise.
            "        int count = r.ReadMapHeader();\n"
          + "        FileBounds.RequireCountFits(count, r.Sequence.Length - r.Consumed, 2, \"x\", \"y\");\n"
          + "        int cap = FileBounds.PreallocFor(count, heapBytesPerElement: 16);\n"
          + "        var pairs = new List<KeyValuePair<string, string>>(cap);",
            // Building one pair is not allocating a collection of them.
            "        pairs.Add(new KeyValuePair<string, string>(k, v));",
        ];

        foreach (string shape in mustFlag)
        {
            var found = new List<string>();
            ScanLines("Probe.cs", shape.Split('\n'), found);
            Assert.True(found.Count > 0, $"the scan no longer sees this:{Environment.NewLine}{shape}");
        }

        foreach (string shape in mustNotFlag)
        {
            var found = new List<string>();
            ScanLines("Probe.cs", shape.Split('\n'), found);
            Assert.True(found.Count == 0,
                $"the scan now reports a bounded allocation:{Environment.NewLine}{shape}{Environment.NewLine}"
              + string.Join(Environment.NewLine, found));
        }
    }

    private static bool NamedBoundAbove(string[] lines, int at, string sized)
    {
        var named = new Regex($@"\b{Regex.Escape(sized)}\b", RegexOptions.None);

        for (int k = Math.Max(0, at - 25); k <= at; k++)
        {
            if (IsComment(lines[k])) continue;

            // Two shapes of guard, and they prove membership differently.
            //
            // A THROWING guard is a statement: it must carry the identifier in its own arguments.
            // Anywhere-on-the-line was not enough — a check over unrelated numbers sitting beside
            // the declaration it does not cover certified an allocation nothing had looked at.
            // The call may wrap, so the arguments are read out of a small window, not one line.
            string window = string.Join(" ", lines.Skip(k).Take(4).Select(static l => l.Trim()));
            for (int idx = window.IndexOf(ThrowingGuard, StringComparison.Ordinal); idx >= 0;
                 idx = window.IndexOf(ThrowingGuard, idx + 1, StringComparison.Ordinal))
            {
                int open = window.IndexOf('(', idx);
                if (open < 0) break;
                string? args = BracketedArgument(window, open);
                if (args is not null && named.IsMatch(args)) return true;
            }

            // A CEILING is an expression — a named constant, or MaxCountThatFits — and the guard
            // is the comparison around it, which is where the identifier stands.
            string line = lines[k];
            if (line.Contains(ThrowingGuard, StringComparison.Ordinal)) continue;
            if (BoundMarkers.Any(mk => line.Contains(mk, StringComparison.Ordinal)) && named.IsMatch(line))
                return true;
        }
        return false;
    }

    private static long MaxCountThatFits(long bytesRemaining, int fileBytes) =>
        (long)Invoke("MaxCountThatFits", bytesRemaining, fileBytes)!;

    private static int PreallocFor(long count, int heapBytes) =>
        (int)Invoke("PreallocFor", count, heapBytes)!;

    private static object? Invoke(string method, params object[] args)
    {
        // FileBounds lives in Ameto.Core, not in the tracing assembly: the rule has to reach the
        // metric readers too, and it could not while it was internal to one project — which is
        // exactly how four unbounded rents there survived a sweep aimed at nine sites next door.
        var m = typeof(Ameto.Core.FileBounds).GetMethod(method,
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)!;
        return m.Invoke(null, args);
    }

    private static string RepoRoot()
    {
        var d = new DirectoryInfo(AppContext.BaseDirectory);
        while (d is not null && !Directory.Exists(Path.Combine(d.FullName, "src", "Ameto.Tracing")))
            d = d.Parent;
        Assert.NotNull(d);
        return d!.FullName;
    }
}
