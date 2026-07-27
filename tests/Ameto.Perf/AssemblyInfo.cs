using Xunit;

// Every probe in this assembly measures a PROCESS-WIDE counter — GC.GetTotalMemory for
// the retention/breakdown probes, GC.GetAllocatedBytesForCurrentThread and CPU time for
// the rest. xUnit parallelises test CLASSES within a collection by default, so two probes
// running at once each measure the other's garbage.
//
// That is not theoretical. Running IndexBuildRetentionProbe and TrigramAccumulatorProbe in
// a single `dotnet test` invocation reports:
//
//     RETAINED by index build        -17,0 MB
//     B. List<int> + last-check      -69,7 MB    -4,4 B/pair    -6,0x smaller
//
// Negative retained memory — the "after" snapshot landed below "before" because the other
// class's collection ran in between. Isolated, the same probes report 147,0 MB and
// 75,6 MB. Anyone re-running the suite to check for a regression would read the sign
// backwards and conclude the opposite of the truth.
//
// Serialising the assembly costs a few seconds on a project that exists only to produce
// numbers, and makes those numbers mean what they say without a "run them one at a time"
// caveat living outside the code.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
