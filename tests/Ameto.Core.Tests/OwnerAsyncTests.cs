namespace Ameto.Core.Tests;

/// <summary>
/// Round-5 review M-A. <see cref="OwnerAsync"/>'s test seam was an <see cref="AsyncLocal{T}"/> its static
/// initializer created, and the type is initialized at the process's first owner-method suspension — the
/// first swap, spill or despill. An out of memory there failed the type for good: every later first
/// suspension threw <see cref="TypeInitializationException"/>, so no swap, spill or despill ran again and
/// log ingest answered 503 or 500 until a restart. The type has no initializer now: the seam is created
/// by the first test that asks for it, and a suspension reads a field that is null in production.
/// </summary>
public sealed class OwnerAsyncTests
{
    [Fact]
    public void OwnerAsync_has_no_static_initializer_that_could_fail()
    {
        Assert.Null(typeof(OwnerAsync).TypeInitializer);
    }

    /// <summary>The seam still reaches the flow that sets it, and what flows from it; cleared, nothing.</summary>
    [Fact]
    public async Task The_seam_reaches_the_flow_that_sets_it()
    {
        static void Suspend() => OwnerAsync.FirstSuspension(typeof(OwnerAsyncTests));

        OwnerAsync.FirstSuspensionFailsForTest.Value = t => t == typeof(OwnerAsyncTests);
        try
        {
            Assert.Throws<OutOfMemoryException>(Suspend);
            await Task.Run(() => Assert.Throws<OutOfMemoryException>(Suspend));   // flows with the context
            OwnerAsync.FirstSuspension(typeof(string));                          // another state machine: untouched
        }
        finally { OwnerAsync.FirstSuspensionFailsForTest.Value = null; }
        Suspend();
    }
}
