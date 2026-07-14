namespace Ameto.Core;

/// <summary>
/// Authorization policy names for the per-user read scopes (Logs / Metrics /
/// Traces / Stats). Defined in Core so the query endpoint mappers that live in
/// separate assemblies (Ameto.Metrics, Ameto.Tracing) — which do not reference
/// Ameto.Server — can gate their endpoints with the very same policy names the
/// server registers in DI.
/// </summary>
public static class ViewPolicies
{
    public const string Logs    = "ViewLogs";
    public const string Metrics = "ViewMetrics";
    public const string Traces  = "ViewTraces";
    public const string Stats   = "ViewStats";
}
