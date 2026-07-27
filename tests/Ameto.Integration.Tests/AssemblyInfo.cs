using Xunit;

// Six classes in this project take IClassFixture<AmetoWebAppFactory>, and each fixture boots a
// whole server: storage engine, alert evaluator, metric and trace engines, plus the background
// services. xUnit parallelises test classes, so those hosts otherwise run at the same time and
// compete for the same cores and disk.
//
// Several assertions here are wall-clock bound rather than deterministic — FullIntegrationTests
// drains through WaitForEventsAsync, which fails the test if ingested events are not queryable
// within ten seconds. That deadline is generous when the suite has the machine to itself and
// much less so with six hosts flushing segments at once, which is how a single unreproducible
// failure appears in one full-solution run and in none of the next four.
//
// This is a different hazard from tests/Ameto.Storage.Tests and tests/Ameto.Perf, which were
// serialised because they assert on process-wide GC counters; the fix happens to be the same
// one. The suite completes in a couple of seconds either way, so the parallelism was not
// buying anything worth a rare red build.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
