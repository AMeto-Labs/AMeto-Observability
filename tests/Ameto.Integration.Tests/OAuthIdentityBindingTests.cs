using Ameto.Server.Auth;
using Microsoft.Extensions.Logging;

namespace Ameto.Integration.Tests;

/// <summary>
/// An OAuth sign-in used to be resolved on the email claim alone. That claim is an attribute
/// of a directory the signer-in may well administer — anyone can stand up an Entra tenant and
/// assert any address in it — so matching on it handed over whichever Ameto account carried the
/// same address, role included.
///
/// The account is now bound to the provider's immutable subject id. A row with no subject yet
/// (created by an admin adding an allowlist entry, or predating the column) adopts the first
/// one to sign in; after that a different subject claiming the same address is refused.
/// </summary>
public sealed class OAuthIdentityBindingTests : IDisposable
{
    private readonly string          _dir;
    private readonly AuthStore       _store;
    private readonly CapturingLogger _log = new();

    public OAuthIdentityBindingTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), "ameto-auth-" + Guid.NewGuid().ToString("N")[..8]);
        _store = new AuthStore(
            new AuthDatabase(_dir),
            new AuthOptions { AdminPassword = "seeded-for-tests" },
            _log);
    }

    /// <summary>Collects formatted log lines so the binding audit trail can be asserted on.</summary>
    private sealed class CapturingLogger : ILogger<AuthStore>
    {
        public readonly List<string> Lines = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter) =>
            Lines.Add(formatter(state, exception));
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* WAL handles may linger */ }
    }

    [Fact]
    public void AnAllowlistedRowAdoptsTheFirstSubjectThatSignsIn()
    {
        // An admin pre-creates the allowlist entry: address known, subject not yet.
        _store.CreateOAuthUser("dana@corp.example", "Dana", "microsoft", "admin");

        var first = _store.FindOrCreateOAuthUser("dana@corp.example", "Dana", "microsoft", "entra-oid-dana");

        Assert.NotNull(first);
        Assert.Equal("admin", first!.Role);
        Assert.Equal("entra-oid-dana", _store.FindOAuthUser("dana@corp.example", "microsoft")!.ProviderSubject);
    }

    [Fact]
    public void ASecondSubjectAssertingTheSameAddressIsRefused()
    {
        _store.CreateOAuthUser("erin@corp.example", "Erin", "microsoft", "admin");
        _store.FindOrCreateOAuthUser("erin@corp.example", "Erin", "microsoft", "entra-oid-erin");

        // Attacker's own tenant, same address asserted, different immutable id.
        var impostor = _store.FindOrCreateOAuthUser("erin@corp.example", "Erin", "microsoft", "attacker-tenant-oid");

        Assert.Null(impostor);
    }

    [Fact]
    public void BindingAnAccountToItsFirstSubjectIsLogged()
    {
        // Adoption is the moment an unbound allowlist row acquires an owner. Under
        // AllowMultiTenant that "owner" may be a stranger, and every sign-in afterwards looks
        // ordinary — this line is the only thing separating the two after the fact.
        _store.CreateOAuthUser("ivan@corp.example", "Ivan", "microsoft", "admin");
        _store.FindOrCreateOAuthUser("ivan@corp.example", "Ivan", "microsoft", "entra-oid-ivan");

        var bound = Assert.Single(_log.Lines, l => l.Contains("bound to subject", StringComparison.Ordinal));
        Assert.Contains("ivan@corp.example", bound, StringComparison.Ordinal);
        Assert.Contains("entra-oid-ivan",    bound, StringComparison.Ordinal);
        Assert.Contains("admin",             bound, StringComparison.Ordinal);

        // An already-bound account signing in again is routine and stays quiet.
        _log.Lines.Clear();
        _store.FindOrCreateOAuthUser("ivan@corp.example", "Ivan", "microsoft", "entra-oid-ivan");
        Assert.DoesNotContain(_log.Lines, l => l.Contains("bound to subject", StringComparison.Ordinal));
    }

    [Fact]
    public void TheBoundSubjectKeepsSigningIn()
    {
        _store.CreateOAuthUser("frank@corp.example", "Frank", "google", "manager");
        _store.FindOrCreateOAuthUser("frank@corp.example", "Frank", "google", "google-sub-frank");

        var again = _store.FindOrCreateOAuthUser("frank@corp.example", "Frank", "google", "google-sub-frank");

        Assert.NotNull(again);
        Assert.Equal("manager", again!.Role);
    }

    [Fact]
    public void ASignInWithoutASubjectIsRefused()
    {
        _store.CreateOAuthUser("gail@corp.example", "Gail", "google", "viewer");

        Assert.Null(_store.FindOrCreateOAuthUser("gail@corp.example", "Gail", "google", ""));
    }

    [Fact]
    public void AutoProvisioningThroughADomainRuleRecordsTheSubject()
    {
        _store.CreateOAuthDomain("google", "corp.example", "viewer");

        var user = _store.FindOrCreateOAuthUser("hana@corp.example", "Hana", "google", "google-sub-hana");

        Assert.NotNull(user);
        Assert.Equal("viewer", user!.Role);
        Assert.Equal("google-sub-hana", user.ProviderSubject);

        // And the freshly provisioned row is bound from the start.
        Assert.Null(_store.FindOrCreateOAuthUser("hana@corp.example", "Hana", "google", "someone-else"));
    }
}
