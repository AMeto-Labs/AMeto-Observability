using Ameto.Core;

namespace Ameto.Tracing.Storage;

/// <summary>
/// THE SPAN BLOOM'S CASE FOLD — <see cref="CaseFold"/>, the table compiled into the binary that the
/// log segment's bloom filter and trigram index fold with as well. It lived here until the log
/// index took it up for the same disagreement (#86 for spans: lowercase is not the equivalence
/// <c>OrdinalIgnoreCase</c> uses); what it is, why it is a table and how it was generated are
/// written there. This name stays so the span code and its tests read as they did, and every
/// member forwards: one table, one <see cref="Fingerprint"/>, one host-drift set per process.
/// </summary>
internal static class SpanBloomFold
{
    /// <inheritdoc cref="CaseFold.Bmp"/>
    internal static char Bmp(char c) => CaseFold.Bmp(c);

    /// <inheritdoc cref="CaseFold.Fingerprint"/>
    internal static ulong Fingerprint => CaseFold.Fingerprint;

    /// <inheritdoc cref="CaseFold.HostDisagrees"/>
    internal static bool HostDisagrees(char c) => CaseFold.HostDisagrees(c);

    /// <inheritdoc cref="CaseFold.HostDisagreementCount"/>
    internal static int HostDisagreementCount => CaseFold.HostDisagreementCount;

    /// <inheritdoc cref="CaseFold.HostDisagreesOnAscii"/>
    internal static bool HostDisagreesOnAscii => CaseFold.HostDisagreesOnAscii;

    /// <inheritdoc cref="CaseFold.UseHostDriftForTest"/>
    internal static CaseFold.DriftSwap UseHostDriftForTest(string marked) => CaseFold.UseHostDriftForTest(marked);

    /// <inheritdoc cref="CaseFold.UseTableForTest"/>
    internal static CaseFold.TableSwap UseTableForTest(char[] table) => CaseFold.UseTableForTest(table);

    /// <inheritdoc cref="CaseFold.CopyOfTable"/>
    internal static char[] CopyOfTable() => CaseFold.CopyOfTable();

    /// <inheritdoc cref="CaseFold.ForgetHostDriftForTest"/>
    internal static void ForgetHostDriftForTest() => CaseFold.ForgetHostDriftForTest();
}
