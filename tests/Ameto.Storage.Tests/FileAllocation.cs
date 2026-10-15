using System.Diagnostics;
using System.Globalization;

namespace Ameto.Storage.Tests;

/// <summary>What the file system has allocated to a file, read on Linux with <c>stat</c> (round-5 H-F).</summary>
internal static class FileAllocation
{
    /// <summary>
    /// The bytes <paramref name="path"/> has on disk — its allocated blocks times their size — as opposed to
    /// its length, which a sparse file has without the blocks behind it. Linux only.
    /// </summary>
    public static long AllocatedBytes(string path)
    {
        var psi = new ProcessStartInfo("stat", ["-c", "%b %B", path])
        {
            RedirectStandardOutput = true,
            RedirectStandardError  = true,
            UseShellExecute        = false,
        };
        using var stat = Process.Start(psi) ?? throw new InvalidOperationException("stat could not be started");
        string output = stat.StandardOutput.ReadToEnd();
        stat.WaitForExit();
        if (stat.ExitCode != 0) throw new InvalidOperationException($"stat failed: {stat.StandardError.ReadToEnd()}");
        var parts = output.Trim().Split(' ');
        return long.Parse(parts[0], CultureInfo.InvariantCulture) * long.Parse(parts[1], CultureInfo.InvariantCulture);
    }
}
