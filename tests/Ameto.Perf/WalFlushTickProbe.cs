using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using Ameto.Core;
using Ameto.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Ameto.Perf;

/// <summary>
/// The WAL flush tick, every 2 s for the life of every hot tier.
///
/// <para>Two costs were being paid for no reason. The mapping-wide msync
/// (<c>MemoryMappedViewAccessor.Flush</c> hands FlushViewOfFile/msync the WHOLE view) scales
/// with the 64 MB mapping, not with the handful of pages a tick actually dirtied — and on an
/// idle server the tick msynced and fsynced anyway, with nothing appended since the last one.
/// The first part is measured here against the same primitive over just the written tail; the
/// second is measured through the real <see cref="WriteAheadLog.Flush"/>.</para>
/// </summary>
public sealed class WalFlushTickProbe
{
    private const long MappingBytes = 64L * 1024 * 1024;   // the production default
    private const int  DirtyBytes   = 64 * 1024;           // a busy 2 s tick, generously

    private readonly ITestOutputHelper _out;
    public WalFlushTickProbe(ITestOutputHelper o) => _out = o;

    [Fact]
    public unsafe void RangeFlushBeatsWholeViewFlush()
    {
        if (!OperatingSystem.IsWindows())
        {
            // The primitive differs (msync); the WAL handles both, but this probe only
            // carries the Windows one. IdleTickCostsNothing below covers every platform.
            _out.WriteLine("skipped: this probe calls FlushViewOfFile directly");
            return;
        }

        string path = Path.Combine(Path.GetTempPath(), $"Ameto-walprobe-{Guid.NewGuid():N}.bin");
        try
        {
            using var fs = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read);
            fs.SetLength(MappingBytes);
            using var mmf = MemoryMappedFile.CreateFromFile(fs, null, MappingBytes,
                MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, leaveOpen: true);
            using var acc = mmf.CreateViewAccessor(0, MappingBytes, MemoryMappedFileAccess.ReadWrite);

            byte* ptr = null;
            acc.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
            try
            {
                const int rounds = 5;
                double wholeNs = double.MaxValue, rangeNs = double.MaxValue;
                var sw = new Stopwatch();

                for (int r = 0; r < rounds; r++)
                {
                    Dirty(ptr, r);
                    sw.Restart();
                    FlushViewOfFile((nint)ptr, (nuint)MappingBytes);
                    sw.Stop();
                    wholeNs = Math.Min(wholeNs, sw.Elapsed.TotalNanoseconds);

                    Dirty(ptr, r + 100);
                    sw.Restart();
                    FlushViewOfFile((nint)ptr, (nuint)DirtyBytes);
                    sw.Stop();
                    rangeNs = Math.Min(rangeNs, sw.Elapsed.TotalNanoseconds);
                }

                // The other half of a tick, and the expensive half: waiting out the drive.
                // It used to run under the WAL's write lock, so the single appender waited
                // with it; it now runs outside.
                double fsyncNs = double.MaxValue;
                for (int r = 0; r < rounds; r++)
                {
                    Dirty(ptr, r + 200);
                    FlushViewOfFile((nint)ptr, (nuint)DirtyBytes);
                    sw.Restart();
                    fs.Flush(flushToDisk: true);
                    sw.Stop();
                    fsyncNs = Math.Min(fsyncNs, sw.Elapsed.TotalNanoseconds);
                }

                _out.WriteLine(
                    $"WAL flush tick, {MappingBytes / (1024 * 1024)} MB mapping, {DirtyBytes / 1024} KB dirty:\n" +
                    $"  msync whole view (before): {wholeNs / 1000,8:F1} µs\n" +
                    $"  msync written tail (after): {rangeNs / 1000,7:F1} µs\n" +
                    $"  fsync the handle          : {fsyncNs / 1000,7:F1} µs\n" +
                    $"  held under the write lock — before: {(wholeNs + fsyncNs) / 1000,7:F1} µs   after: {rangeNs / 1000,7:F1} µs");
            }
            finally
            {
                acc.SafeMemoryMappedViewHandle.ReleasePointer();
            }
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }

        static void Dirty(byte* p, int seed)
        {
            for (int i = 0; i < DirtyBytes; i += 512) p[i] = (byte)(seed + i);
        }
    }

    [Fact]
    public void IdleTickCostsNothing()
    {
        string path = Path.Combine(Path.GetTempPath(), $"Ameto-walidle-{Guid.NewGuid():N}.wal");
        try
        {
            using var wal = WriteAheadLog.Open(path, new NodeId(0), new SegmentId(1UL), MappingBytes);

            for (int i = 0; i < 500; i++)
                wal.Append(i, Ameto.Core.LogLevel.Information, (ushort)(i % 8), "tmpl", new byte[64]);

            // One dirty tick, then the idle ones — the shape of a server between bursts.
            var sw = Stopwatch.StartNew();
            wal.Flush();
            sw.Stop();
            double dirtyUs = sw.Elapsed.TotalMicroseconds;
            long   after   = wal.RangeFlushCount;

            const int idle = 200;
            sw.Restart();
            for (int i = 0; i < idle; i++) wal.Flush();
            sw.Stop();
            double idleUs = sw.Elapsed.TotalMicroseconds / idle;

            _out.WriteLine(
                $"WriteAheadLog.Flush, 64 MB mapping, ~40 KB appended:\n" +
                $"  tick with appends : {dirtyUs,8:F1} µs\n" +
                $"  tick with nothing : {idleUs,8:F3} µs   (before: a whole-view msync + fsync, every time)");

            Assert.Equal(after, wal.RangeFlushCount);   // the idle ticks did no I/O
        }
        finally
        {
            foreach (string p in new[] { path, path + ".pool" })
                try { File.Delete(p); } catch { }
        }
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool FlushViewOfFile(nint lpBaseAddress, nuint dwNumberOfBytesToFlush);
}
