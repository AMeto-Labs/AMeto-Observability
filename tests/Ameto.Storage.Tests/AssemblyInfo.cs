using Xunit;

// MetricReaderStreamingProbe measures live bytes with GC.GetTotalMemory — a PROCESS-WIDE
// counter — and then ASSERTS on the result (streamed < materialised / 2). xUnit
// parallelises test classes by default, and this project has twelve of them, so another
// class allocating or collecting inside the probe's measurement window lands directly in
// its reading.
//
// That makes it a flaky test rather than merely a misleading printout: an unrelated class
// allocating during the "streamed" pass inflates it toward the threshold, and a collection
// during the "materialised" pass deflates the number it is compared against. Either way
// the failure has nothing to do with the reader being measured.
//
// The same hazard, and the same fix, as tests/Ameto.Perf/AssemblyInfo.cs — where running
// two probe classes together produced negative megabytes. This suite completes in about
// two seconds, so serialising it costs nothing worth counting.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
