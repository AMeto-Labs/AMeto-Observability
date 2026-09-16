namespace Ameto.Core;

/// <summary>
/// A component holding memory that can be given back on demand, and that the
/// <c>RamPressureService</c> can therefore ask to let go under pressure.
///
/// <para>It exists for one shape the pressure path could not reach: memory held OUTSIDE the
/// managed heap by a component the pressure path cannot reference. The segment-index cache is
/// exactly that — it lives in <c>Ameto.Indexing</c>, which references <c>Ameto.Storage</c>, so
/// <c>RamPressureService</c> (Storage) cannot call it directly; and its bloom bits are
/// <c>NativeMemory</c>, so no collection the pressure path forces can reclaim them however hard
/// it tries. Both halves are fixed by inverting the dependency through this interface, which
/// lives in Core where both sides can see it.</para>
/// </summary>
public interface IMemoryShedder
{
    /// <summary>
    /// Total bytes <see cref="Shed"/> would release — managed and native together.
    /// </summary>
    long ShedableBytes { get; }

    /// <summary>
    /// The part of <see cref="ShedableBytes"/> that is NOT on the managed heap.
    ///
    /// <para>Reported separately because the caller's "is this pressure ours?" test already
    /// counts the managed heap via <c>GC.GetGCMemoryInfo().HeapSizeBytes</c>. Adding the whole
    /// figure there would count the managed part twice; adding nothing would leave native bytes
    /// invisible to the one guard that decides whether relieving pressure is worthwhile — which
    /// is the blind spot this interface exists to close.</para>
    /// </summary>
    long ShedableNativeBytes { get; }

    /// <summary>
    /// Releases what this component is holding and returns how many bytes that was.
    ///
    /// <para>"Released" means the component has given the bytes up, not that the process has
    /// already handed them back: a holder still using part of it (a lease, an in-flight query)
    /// keeps that part alive until it is done, exactly as an eviction does. Implementations must
    /// never wait on a caller of their own to finish, and must be safe to call concurrently with
    /// normal use.</para>
    /// </summary>
    long Shed();
}

/// <summary>
/// The process's registered <see cref="IMemoryShedder"/>s, so the RAM-pressure loop can ask
/// every one of them to let go without referencing any of them.
///
/// <para>Registrations are weak. A registrant that is collected without unregistering — a test
/// host's cache, an engine built and dropped — is pruned rather than kept alive by this list,
/// which is what stops a process-wide static from turning every short-lived host into a leak.
/// Unregistering explicitly (dispose the token) is still the contract; the weak reference is the
/// backstop, not the mechanism.</para>
/// </summary>
public static class MemoryShedRegistry
{
    private static readonly Lock                              _lock    = new();
    private static readonly List<WeakReference<IMemoryShedder>> _entries = [];

    /// <summary>
    /// Registers <paramref name="shedder"/> until the returned token is disposed.
    /// </summary>
    public static IDisposable Register(IMemoryShedder shedder)
    {
        ArgumentNullException.ThrowIfNull(shedder);
        var entry = new WeakReference<IMemoryShedder>(shedder);
        lock (_lock)
        {
            PruneLocked();
            _entries.Add(entry);
        }
        return new Registration(entry);
    }

    /// <summary>Live registrations — for diagnostics and for tests that assert unregistration.</summary>
    public static int RegisteredCount
    {
        get { lock (_lock) { PruneLocked(); return _entries.Count; } }
    }

    /// <summary>Total bytes every registered shedder would release.</summary>
    public static long ShedableBytes
    {
        get
        {
            long total = 0;
            foreach (var s in Snapshot())
            {
                try { total += s.ShedableBytes; } catch { /* one shedder must not blind the rest */ }
            }
            return total;
        }
    }

    /// <summary>
    /// The off-heap part of <see cref="ShedableBytes"/> — what a forced collection could never
    /// reclaim on its own. See <see cref="IMemoryShedder.ShedableNativeBytes"/>.
    /// </summary>
    public static long ShedableNativeBytes
    {
        get
        {
            long total = 0;
            foreach (var s in Snapshot())
            {
                try { total += s.ShedableNativeBytes; } catch { }
            }
            return total;
        }
    }

    /// <summary>
    /// Asks every registered shedder to let go, and returns the total it reported.
    ///
    /// <para>Never throws: this runs from the pressure loop, where a failure to shed one
    /// component must not stop the flush and the collection that follow it.</para>
    /// </summary>
    public static long Shed()
    {
        long total = 0;
        foreach (var s in Snapshot())
        {
            try { total += s.Shed(); } catch { }
        }
        return total;
    }

    /// <summary>
    /// The live shedders, copied out so they are called with no lock held — a shed takes the
    /// registrant's own lock, and holding this one across that call would order two locks.
    /// </summary>
    private static IMemoryShedder[] Snapshot()
    {
        lock (_lock)
        {
            PruneLocked();
            if (_entries.Count == 0) return [];

            var live = new IMemoryShedder[_entries.Count];
            int n = 0;
            for (int i = 0; i < _entries.Count; i++)
                if (_entries[i].TryGetTarget(out var s)) live[n++] = s;
            return n == _entries.Count ? live : live[..n];
        }
    }

    private static void PruneLocked()
    {
        for (int i = _entries.Count - 1; i >= 0; i--)
            if (!_entries[i].TryGetTarget(out _)) _entries.RemoveAt(i);
    }

    private sealed class Registration : IDisposable
    {
        private WeakReference<IMemoryShedder>? _entry;

        public Registration(WeakReference<IMemoryShedder> entry) => _entry = entry;

        public void Dispose()
        {
            var entry = Interlocked.Exchange(ref _entry, null);
            if (entry is null) return;                  // idempotent
            lock (_lock) _entries.Remove(entry);        // by reference identity
        }
    }
}
