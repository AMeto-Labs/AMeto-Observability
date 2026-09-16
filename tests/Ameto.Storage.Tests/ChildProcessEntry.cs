using Ameto.Core;

namespace Ameto.Storage.Tests;

/// <summary>
/// Entry point of this test assembly when it is started as a CHILD process:
/// <c>dotnet exec Ameto.Storage.Tests.dll &lt;command&gt;</c>.
///
/// <para>Some behaviour can only be observed in a process that was STARTED under a setting — the
/// GC reads its memory limits once, at startup, so a heap hard limit or a container-sized physical
/// limit cannot be applied to the test host after the fact without destabilising every other test
/// running in it. A test starts this assembly again under the setting and reads what it prints.</para>
///
/// <para>The test SDK normally generates an empty <c>Main</c>; it is switched off in the project
/// file (<c>GenerateProgramFile</c>) so this one is used. The test runner never calls it.</para>
/// </summary>
internal static class ChildProcessEntry
{
    /// <summary>Prints <see cref="MemoryBudgets.Current"/> as <c>key=value</c> lines.</summary>
    internal const string MemoryBudgetsCommand = "memory-budgets";

    public static int Main(string[] args)
    {
        if (args.Length == 1 && args[0] == MemoryBudgetsCommand)
        {
            var b = MemoryBudgets.Current();
            Console.WriteLine($"managedLimit={b.ManagedLimitBytes}");
            Console.WriteLine($"physicalLimit={b.PhysicalLimitBytes}");
            Console.WriteLine($"managedBuild={b.ManagedBuildBytes}");
            Console.WriteLine($"nativeTier={b.NativeTierBytes}");
            Console.WriteLine($"indexCache={b.IndexCacheBytes}");
            // The cache's native share is the one budget whose BASE is the interesting part —
            // it is taken of the container, not of the heap limit the rest of the cache is a
            // share of — so a child that cannot print it cannot prove which base was used.
            Console.WriteLine($"indexCacheNative={b.IndexCacheNativeBytes}");
            return 0;
        }

        Console.Error.WriteLine($"usage: {MemoryBudgetsCommand}");
        return 2;
    }
}
