namespace Ameto.Core;

/// <summary>
/// Whether a store can give a TRUE answer to a read right now — as distinct from the answer
/// itself, which a store that cannot answer gives as EMPTY (or partial) so that a request arriving
/// late fails soft instead of throwing out of the middle of a response.
///
/// <para><b>Why the answer cannot carry this by itself.</b> "No spans", "no series" and "the store
/// has closed" are the same empty list. A caller that ACTS on the answer — the alert evaluator,
/// which turns it into a number and compares the number to a threshold — reads the closed store as
/// a real 0, and a real 0 resolves every firing "&gt;" rule (an Ok notification for an incident
/// nobody resolved) and fires every "&lt;" rule. So the store says it separately, and the caller
/// asks. See issue #95.</para>
///
/// <para><b>The states only move forward:</b> <see cref="Loading"/> → <see cref="Available"/> →
/// <see cref="Closed"/>. That is what makes the question answerable around an asynchronous read
/// with two volatile reads and no lock: an answer taken while the store was still loading was
/// preceded by a <see cref="Loading"/> answer to a question asked BEFORE the read, and an answer
/// taken after the store closed is followed by a <see cref="Closed"/> answer to one asked AFTER
/// it. A caller that asks both times and acts only on <see cref="Available"/> twice cannot act on
/// either.</para>
/// </summary>
public enum QueryAvailability : byte
{
    /// <summary>Every read reflects everything the store holds.</summary>
    Available = 0,

    /// <summary>
    /// The store is still discovering what it holds on disk (its cold tier is scanned in the
    /// background after startup): a read now sees only what arrived since the start — a PART.
    /// </summary>
    Loading = 1,

    /// <summary>The store's teardown has shut the door: a read now answers EMPTY. Final.</summary>
    Closed = 2,
}

/// <summary>
/// A store that can say whether its answers are true right now — see <see cref="QueryAvailability"/>.
///
/// <para>A DEFAULT of <see cref="QueryAvailability.Available"/>, so a store that has no lifecycle
/// (a test fake, an in-memory stand-in) need not write one down. The engines that DO close — the
/// trace and metric storage engines — implement it as a volatile read of the fence their read paths
/// already check, so asking costs nothing on the read path.</para>
/// </summary>
public interface IQueryAvailability
{
    /// <summary>Whether a read issued now gets a true answer. Never blocks, never allocates.</summary>
    QueryAvailability Availability => QueryAvailability.Available;
}
