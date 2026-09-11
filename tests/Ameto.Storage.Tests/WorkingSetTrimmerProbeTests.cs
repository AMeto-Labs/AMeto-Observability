using System.Runtime.ExceptionServices;
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
/// <see cref="AppDomain.FirstChanceException"/> rather than looking for something to catch. On
/// Windows <c>TrimAllocator</c> is a no-op and the assertion is trivially true; the test earns
/// its keep on the Linux/musl CI leg and on any glibc box, where a regression would fire it.</para>
/// </summary>
public sealed class WorkingSetTrimmerProbeTests
{
    [Fact]
    public void Repeated_allocator_trims_throw_nothing()
    {
        int thrown = 0;
        void OnFirstChance(object? _, FirstChanceExceptionEventArgs e)
        {
            if (e.Exception is EntryPointNotFoundException or DllNotFoundException)
                Interlocked.Increment(ref thrown);
        }

        WorkingSetTrimmer.TrimAllocator();                 // probe once, outside the window
        AppDomain.CurrentDomain.FirstChanceException += OnFirstChance;
        try
        {
            for (int i = 0; i < 500; i++) WorkingSetTrimmer.TrimAllocator();
        }
        finally
        {
            AppDomain.CurrentDomain.FirstChanceException -= OnFirstChance;
        }

        Assert.Equal(0, thrown);
    }

    /// <summary>
    /// Even the FIRST call must not throw — the probe is a lookup, not a call that fails.
    /// </summary>
    [Fact]
    public void Probe_answers_without_throwing_on_any_platform()
    {
        int thrown = 0;
        void OnFirstChance(object? _, FirstChanceExceptionEventArgs e)
        {
            if (e.Exception is EntryPointNotFoundException or DllNotFoundException)
                Interlocked.Increment(ref thrown);
        }

        AppDomain.CurrentDomain.FirstChanceException += OnFirstChance;
        bool has;
        try { has = WorkingSetTrimmer.HasMallocTrim; }
        finally { AppDomain.CurrentDomain.FirstChanceException -= OnFirstChance; }

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
}
