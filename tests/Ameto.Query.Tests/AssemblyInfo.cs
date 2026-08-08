using Xunit;

// IndexGroupPrefilterTests.EveryGroupReadsItsBloomSectionOnce asserts an EXACT count off
// SegmentReader.PooledSectionRents — a process-wide static. xUnit parallelises test classes by
// default, so any other class in this assembly opening a segment inside that measurement window
// lands in the reading and the assertion fails on someone else's work.
//
// It is an equality, not a threshold, so there is no slack to absorb that: one stray rent from a
// neighbouring class is a failure. Serialising is the only fix that keeps the assertion exact,
// and an exact count is the point — the regression it catches (renting a section twice per group
// instead of once) is invisible to every other instrument at test-sized sections, because under
// 1 MB ArrayPool absorbs the second rent for free.
//
// The same hazard and the same fix as tests/Ameto.Perf/AssemblyInfo.cs and
// tests/Ameto.Storage.Tests/AssemblyInfo.cs. This suite is dominated by parser and evaluator
// tests that finish in milliseconds, so serialising it costs nothing worth counting.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
