namespace Ameto.Alerts;

/// <summary>
/// How the alert evaluator treats the stores it reads — the <c>Ameto:Alerts</c> settings other than
/// <c>Enabled</c>, which decides whether there is an evaluator at all.
/// </summary>
public sealed class AlertEvaluatorOptions
{
    /// <summary>
    /// <c>Ameto:Alerts:EvaluateOnDegradedStore</c> (#94). <b>False, the default:</b> a rule over a
    /// store that is <see cref="Ameto.Core.QueryAvailability.Degraded"/> — its startup scan could not
    /// load everything on disk, and it stays partial until a restart — is skipped exactly like a rule
    /// over a store still loading: its state, last value and evaluation time are left as they are,
    /// nothing is sent, and a warning says so once a minute. <b>True:</b> such rules are evaluated on
    /// what the store has, as every rule was before the state existed — for an operator who would
    /// rather have rules that can misfire on a window the store failed to load (a "&lt;" rule can
    /// fire, a "&gt;" rule can resolve) than rules that say nothing until the restart. The store's
    /// own Error at startup is then the only word about it.
    ///
    /// <para>A store still loading, or closed, is skipped either way: the first ends by itself, and
    /// the second answers empty.</para>
    /// </summary>
    public bool EvaluateOnDegradedStore { get; init; }
}
