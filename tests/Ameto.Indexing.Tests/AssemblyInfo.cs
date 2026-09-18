using Xunit;

// Several tests here assert on POOL behaviour — that a build allocates nothing once the
// IndexBuildPool is warm, that a disposed filter takes nothing out of ArrayPool<char>.Shared.
// xUnit runs test classes in parallel by default, and the pools are process-wide: a parity
// test renting slabs on another thread at the same moment empties the bucket the measured
// build was about to hit, and the assertion reads a fresh allocation that no code path made.
// The whole assembly runs in about a second, so serialising it costs nothing and makes those
// numbers mean what they say.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
