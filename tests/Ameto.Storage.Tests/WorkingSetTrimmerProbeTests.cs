using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using Ameto.Storage;
using Xunit;

namespace Ameto.Storage.Tests;

/// <summary>
/// <c>malloc_trim</c> is a glibc extension, and the shipped container image is Alpine (musl)
/// with jemalloc preloaded — neither exports it. The <c>DllImport</c> this replaced therefore
/// threw <see cref="EntryPointNotFoundException"/> on EVERY call, once per 30 s RamPressureService
/// tick for the life of the process, and the throw was swallowed so nobody saw it.
///
/// <para>A swallowed exception is still a thrown one, so these watch
/// <see cref="AppDomain.FirstChanceException"/> rather than looking for something to catch.</para>
///
/// <para>The platform-independent tests go through <see cref="MallocTrimBinding"/> with a lookup the
/// test controls. CI runs this suite on Windows only, where <c>TrimAllocator</c> is a no-op — so a
/// test that only called it would pass whatever the binding did. The same goes for the
/// <c>/proc/self/statm</c> parser, which only ever reads a real file on Linux.</para>
/// </summary>
public sealed unsafe class WorkingSetTrimmerProbeTests
{
    private static int CountFirstChance(Action action)
    {
        int thrown = 0;
        void OnFirstChance(object? _, FirstChanceExceptionEventArgs e)
        {
            if (e.Exception is EntryPointNotFoundException or DllNotFoundException)
                Interlocked.Increment(ref thrown);
        }

        AppDomain.CurrentDomain.FirstChanceException += OnFirstChance;
        try     { action(); }
        finally { AppDomain.CurrentDomain.FirstChanceException -= OnFirstChance; }
        return thrown;
    }

    // ── The binding, with a lookup the test controls ─────────────────────────────────────

    [Fact]
    public void An_absent_export_is_looked_up_once_and_trims_are_free()
    {
        int lookups = 0;
        var binding = new MallocTrimBinding(() => { Interlocked.Increment(ref lookups); return 0; });

        int thrown = CountFirstChance(() =>
        {
            for (int i = 0; i < 500; i++) binding.Trim();
        });

        Assert.Equal(0, thrown);
        Assert.Equal(1, lookups);
        Assert.False(binding.IsAvailable);
        Assert.Equal(1, lookups);                     // asking again does not look again
    }

    /// <summary>
    /// The regression this pins, in the shape it actually had: a lookup that FAILS BY THROWING —
    /// what a <c>DllImport</c> of a missing export does — must throw once, not once per trim.
    /// </summary>
    [Fact]
    public void A_lookup_that_throws_throws_once_not_once_per_trim()
    {
        int lookups = 0;
        var binding = new MallocTrimBinding(() =>
        {
            Interlocked.Increment(ref lookups);
            throw new EntryPointNotFoundException("malloc_trim");
        });

        int thrown = CountFirstChance(() =>
        {
            for (int i = 0; i < 500; i++) binding.Trim();
        });

        Assert.Equal(1, lookups);
        Assert.Equal(1, thrown);
        Assert.False(binding.IsAvailable);
    }

    private static int s_fakeTrimCalls;

    [UnmanagedCallersOnly]
    private static int FakeMallocTrim(nuint pad)
    {
        Interlocked.Increment(ref s_fakeTrimCalls);
        return 1;
    }

    [Fact]
    public void A_present_export_is_looked_up_once_and_called_on_every_trim()
    {
        s_fakeTrimCalls = 0;
        int lookups = 0;
        var binding = new MallocTrimBinding(() =>
        {
            Interlocked.Increment(ref lookups);
            return (nint)(delegate* unmanaged<nuint, int>)&FakeMallocTrim;
        });

        for (int i = 0; i < 500; i++) binding.Trim();

        Assert.Equal(1, lookups);
        Assert.Equal(500, s_fakeTrimCalls);
        Assert.True(binding.IsAvailable);
    }

    // ── The real binding, on whatever platform this runs ─────────────────────────────────

    [Fact]
    public void Repeated_allocator_trims_throw_nothing()
    {
        WorkingSetTrimmer.TrimAllocator();                 // probe once, outside the window
        int thrown = CountFirstChance(() =>
        {
            for (int i = 0; i < 500; i++) WorkingSetTrimmer.TrimAllocator();
        });

        Assert.Equal(0, thrown);
    }

    /// <summary>Even the FIRST call must not throw — the probe is a lookup, not a call that fails.</summary>
    [Fact]
    public void Probe_answers_without_throwing_on_any_platform()
    {
        bool has = false;
        int thrown = CountFirstChance(() => has = WorkingSetTrimmer.HasMallocTrim);

        Assert.Equal(0, thrown);
        Assert.Equal(has, WorkingSetTrimmer.HasMallocTrim);   // stable answer, probed once
        if (!OperatingSystem.IsLinux()) Assert.False(has);    // only Linux can have it
    }

    /// <summary>The pressure path must stay safe to call anywhere, including Windows.</summary>
    [Fact]
    public void Full_trim_is_safe_to_call()
    {
        WorkingSetTrimmer.TryTrim();
        WorkingSetTrimmer.TryTrim();
    }

    // ── /proc/self/statm ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// <c>size resident shared text lib data dt</c>, in pages. Field 6 — data — is private data +
    /// stack. A parser that read any other field, or none, would silently report something else
    /// as processPrivateBytes on the Linux container.
    /// </summary>
    [Theory]
    [InlineData(4096)]
    [InlineData(16384)]
    public void Statm_data_field_is_the_sixth_in_pages(int pageSize)
    {
        Assert.Equal(4321L * pageSize, ProcessMemoryInfo.ParseStatmDataBytes("1234 567 89 10 0 4321 0\n"u8, pageSize));
    }

    [Fact]
    public void Statm_without_a_trailing_field_still_parses()
    {
        Assert.Equal(6L * 4096, ProcessMemoryInfo.ParseStatmDataBytes("1 2 3 4 5 6"u8, 4096));
    }

    [Theory]
    [InlineData("")]
    [InlineData("1234 567 89")]
    [InlineData("1234 567 89 10 0 x 0\n")]
    [InlineData("1234 567 89 10 0 -5 0\n")]
    public void Malformed_statm_answers_zero_so_the_caller_falls_back(string line)
    {
        Assert.Equal(0, ProcessMemoryInfo.ParseStatmDataBytes(System.Text.Encoding.ASCII.GetBytes(line), 4096));
    }
}
