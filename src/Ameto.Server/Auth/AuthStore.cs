using System.Security.Cryptography;
using System.Text;
using Microsoft.Data.Sqlite;
using Ameto.Ingestion;

namespace Ameto.Server.Auth;

// ── Records ────────────────────────────────────────────────────────────────────

internal sealed record UserRecord(
    string Id,
    string Username,
    string DisplayName,
    string Email,
    string Provider,
    string Role,
    ViewPermissions Permissions,
    DateTimeOffset CreatedAt)
{
    /// <summary>
    /// The provider's immutable subject id (Google <c>sub</c>, Entra <c>oid</c>) for OAuth
    /// users; empty for local accounts and for OAuth rows created before subject binding.
    /// </summary>
    public string ProviderSubject { get; init; } = "";
}

internal sealed record ApiKeyRecord(
    string Id,
    string Name,
    string Description,
    ApiKeyPermissions Permissions,
    string KeyHash,
    string CreatedBy,
    DateTimeOffset CreatedAt)
{
    // Full key — only populated immediately after creation; never persisted.
    public string? Key { get; init; }
}

/// <summary>An OAuth domain allowlist rule: any email @Domain via Provider may
/// sign in, getting <see cref="Role"/> and <see cref="Permissions"/> by default.</summary>
internal sealed record OAuthDomainRecord(
    string Id,
    string Provider,
    string Domain,
    string Role,
    ViewPermissions Permissions,
    DateTimeOffset CreatedAt);

// ── Store ──────────────────────────────────────────────────────────────────────

internal sealed class AuthStore
{
    private readonly AuthDatabase _db;
    private readonly AuthOptions  _opts;
    private readonly Microsoft.Extensions.Logging.ILogger<AuthStore> _logger;

    public AuthStore(AuthDatabase db, AuthOptions opts, Microsoft.Extensions.Logging.ILogger<AuthStore> logger)
    {
        _db     = db;
        _opts   = opts;
        _logger = logger;
        EnsureSeedAdmin();
    }

    // ── Local auth ────────────────────────────────────────────────────────────

    public bool ValidateUser(string username, string password)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            SELECT password_hash, salt FROM users
            WHERE username = @u COLLATE NOCASE AND provider = 'local'
            """;
        cmd.Parameters.AddWithValue("@u", username);
        using var r = cmd.ExecuteReader();
        if (!r.Read()) return false;
        var storedHash = r.GetString(0);
        var salt       = Convert.FromBase64String(r.GetString(1));
        return CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(storedHash),
            Encoding.UTF8.GetBytes(HashPassword(password, salt)));
    }

    // ── OAuth auth ────────────────────────────────────────────────────────────

    /// <summary>
    /// Finds an OAuth user by exact email + provider. Returns null when the email
    /// is not in the per-email allowlist.
    /// </summary>
    public UserRecord? FindOAuthUser(string email, string provider)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            SELECT id, username, display_name, email, provider, role, created_at, permissions, provider_subject
            FROM users
            WHERE email = @e COLLATE NOCASE AND provider = @p
            """;
        cmd.Parameters.AddWithValue("@e", email);
        cmd.Parameters.AddWithValue("@p", provider);
        using var r = cmd.ExecuteReader();
        if (!r.Read()) return null;
        return MapUser(r);
    }

    /// <summary>
    /// Resolves an OAuth sign-in: exact per-email allowlist first, then the
    /// domain allowlist. When only a domain rule matches, the user is
    /// auto-provisioned (so subsequent sign-ins and admin management use the
    /// per-email path). Returns null when neither matches (sign-in refused).
    /// </summary>
    /// <param name="subject">
    /// The provider's immutable subject id for this identity (Google <c>sub</c>, Entra
    /// <c>oid</c>). It — not the email — is what actually identifies the account: an email
    /// claim is a mutable, tenant-controlled attribute, so matching on it alone lets anyone
    /// who can assert an address take over the matching row. The first sign-in for a stored
    /// row binds the subject; from then on a mismatch is refused, so a second identity
    /// asserting the same address cannot inherit the account.
    /// </param>
    public UserRecord? FindOrCreateOAuthUser(string email, string displayName, string provider, string subject)
    {
        if (string.IsNullOrEmpty(subject)) return null; // no stable identity — refuse

        var existing = FindOAuthUser(email, provider);
        if (existing is not null)
        {
            if (existing.ProviderSubject.Length == 0)
            {
                // Row predates subject binding (or was created from the admin allowlist):
                // adopt this subject as the account's identity from here on.
                //
                // This is the moment the account acquires an owner, so it is logged. Until it
                // happens the row belongs to whoever signs in first — fine when the provider is
                // pinned to one tenant, but under AllowMultiTenant "first" can be a stranger,
                // and every sign-in afterwards looks ordinary. The binding line is what makes
                // that distinguishable after the fact.
                BindProviderSubject(existing.Id, subject);
                _logger.LogInformation(
                    "OAuth account {Email} ({Provider}, role {Role}) bound to subject {Subject} on first sign-in",
                    existing.Email, provider, existing.Role, subject);
                return existing with { ProviderSubject = subject };
            }

            if (!string.Equals(existing.ProviderSubject, subject, StringComparison.Ordinal))
            {
                _logger.LogWarning(
                    "Refused an OAuth sign-in for {Email} via {Provider}: the account is bound to a " +
                    "different {Provider} subject. Another identity is asserting this address.",
                    email, provider, provider);
                return null;
            }
            return existing;
        }

        // Extract the host part (after the last '@') without allocating when empty.
        var at = email.LastIndexOf('@');
        if (at <= 0 || at >= email.Length - 1) return null;
        var domain = email[(at + 1)..].ToLowerInvariant();

        var rule = FindOAuthDomain(provider, domain);
        if (rule is null) return null;

        // Auto-provision with the domain rule's default role + permissions so the
        // user appears in the users list and can be managed / re-scoped afterwards.
        return CreateOAuthUser(email, displayName, provider, rule.Role, rule.Permissions, subject);
    }

    /// <summary>Binds a provider subject to a row that does not have one yet (first sign-in).</summary>
    private void BindProviderSubject(string id, string subject)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "UPDATE users SET provider_subject = @s WHERE id = @id AND provider_subject = ''";
        cmd.Parameters.AddWithValue("@s",  subject);
        cmd.Parameters.AddWithValue("@id", id);
        cmd.ExecuteNonQuery();
    }

    /// <summary>Looks up a domain allowlist rule for a provider + domain (case-insensitive).</summary>
    private OAuthDomainRecord? FindOAuthDomain(string provider, string domain)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            SELECT id, provider, domain, role, permissions, created_at
            FROM oauth_domains
            WHERE provider = @p AND domain = @d COLLATE NOCASE
            """;
        cmd.Parameters.AddWithValue("@p", provider);
        cmd.Parameters.AddWithValue("@d", domain);
        using var r = cmd.ExecuteReader();
        if (!r.Read()) return null;
        return new OAuthDomainRecord(
            r.GetString(0), r.GetString(1), r.GetString(2),
            r.GetString(3), (ViewPermissions)r.GetInt32(4), DateTimeOffset.Parse(r.GetString(5)));
    }

    // ── Users: list / create / delete ─────────────────────────────────────────

    /// <summary>
    /// Looks up a user by username (local) or email (OAuth).
    /// Used by the refresh endpoint to verify the account still exists.
    /// </summary>
    public UserRecord? FindByUsernameOrEmail(string username, string email)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            SELECT id, username, display_name, email, provider, role, created_at, permissions, provider_subject
            FROM users
            WHERE username = @u COLLATE NOCASE
               OR (email != '' AND email = @e COLLATE NOCASE)
            LIMIT 1
            """;
        cmd.Parameters.AddWithValue("@u", username);
        cmd.Parameters.AddWithValue("@e", string.IsNullOrWhiteSpace(email) ? "\u0000" : email);
        using var r = cmd.ExecuteReader();
        if (!r.Read()) return null;
        return MapUser(r);
    }

    public IReadOnlyList<UserRecord> ListUsers()
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            SELECT id, username, display_name, email, provider, role, created_at, permissions, provider_subject
            FROM users ORDER BY created_at
            """;
        using var r    = cmd.ExecuteReader();
        var result = new List<UserRecord>();
        while (r.Read()) result.Add(MapUser(r));
        return result;
    }

    public UserRecord? GetUser(string id)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            SELECT id, username, display_name, email, provider, role, created_at, permissions, provider_subject
            FROM users WHERE id = @id
            """;
        cmd.Parameters.AddWithValue("@id", id);
        using var r = cmd.ExecuteReader();
        if (!r.Read()) return null;
        return MapUser(r);
    }

    public UserRecord CreateUser(string username, string password, string role,
        ViewPermissions permissions = ViewPermissions.All)
    {
        var salt = RandomNumberGenerator.GetBytes(16);
        var hash = HashPassword(password, salt);
        var rec  = new UserRecord(
            Guid.NewGuid().ToString("N"), username, username, "", "local",
            NormaliseRole(role), permissions, DateTimeOffset.UtcNow);

        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO users (id, username, display_name, email, provider, password_hash, salt, role, permissions, created_at)
            VALUES (@id, @u, @dn, '', 'local', @h, @s, @r, @perm, @ca)
            """;
        cmd.Parameters.AddWithValue("@id", rec.Id);
        cmd.Parameters.AddWithValue("@u",  rec.Username);
        cmd.Parameters.AddWithValue("@dn", rec.DisplayName);
        cmd.Parameters.AddWithValue("@h",  hash);
        cmd.Parameters.AddWithValue("@s",  Convert.ToBase64String(salt));
        cmd.Parameters.AddWithValue("@r",  rec.Role);
        cmd.Parameters.AddWithValue("@perm", (int)rec.Permissions);
        cmd.Parameters.AddWithValue("@ca", rec.CreatedAt.ToString("O"));
        cmd.ExecuteNonQuery();
        return rec;
    }

    /// <summary>
    /// Creates an OAuth user entry (email-allowlist approach). <paramref name="subject"/> is the
    /// provider's immutable subject id; empty when an admin pre-creates an allowlist entry, in
    /// which case the account binds to whichever subject first signs in as that address.
    /// </summary>
    public UserRecord CreateOAuthUser(string email, string displayName, string provider, string role,
        ViewPermissions permissions = ViewPermissions.All, string subject = "")
    {
        var username = $"{provider}:{email.ToLowerInvariant()}";
        var rec = new UserRecord(
            Guid.NewGuid().ToString("N"), username, displayName, email.ToLowerInvariant(),
            provider, NormaliseRole(role), permissions, DateTimeOffset.UtcNow)
        {
            ProviderSubject = subject,
        };

        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO users (id, username, display_name, email, provider, password_hash, salt, role, permissions, created_at, provider_subject)
            VALUES (@id, @u, @dn, @e, @p, '', '', @r, @perm, @ca, @sub)
            """;
        cmd.Parameters.AddWithValue("@id", rec.Id);
        cmd.Parameters.AddWithValue("@u",  rec.Username);
        cmd.Parameters.AddWithValue("@dn", rec.DisplayName);
        cmd.Parameters.AddWithValue("@e",  rec.Email);
        cmd.Parameters.AddWithValue("@p",  rec.Provider);
        cmd.Parameters.AddWithValue("@r",  rec.Role);
        cmd.Parameters.AddWithValue("@perm", (int)rec.Permissions);
        cmd.Parameters.AddWithValue("@ca", rec.CreatedAt.ToString("O"));
        cmd.Parameters.AddWithValue("@sub", rec.ProviderSubject);
        cmd.ExecuteNonQuery();
        return rec;
    }

    public bool DeleteUser(string id)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM users WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        return cmd.ExecuteNonQuery() > 0;
    }

    public bool UpdateUserRole(string id, string role)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "UPDATE users SET role = @r WHERE id = @id";
        cmd.Parameters.AddWithValue("@r",  NormaliseRole(role));
        cmd.Parameters.AddWithValue("@id", id);
        return cmd.ExecuteNonQuery() > 0;
    }

    /// <summary>Updates display name, role and view permissions for a user.</summary>
    public bool UpdateUser(string id, string displayName, string role, ViewPermissions permissions)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "UPDATE users SET display_name = @dn, role = @r, permissions = @perm WHERE id = @id";
        cmd.Parameters.AddWithValue("@dn", displayName);
        cmd.Parameters.AddWithValue("@r",  NormaliseRole(role));
        cmd.Parameters.AddWithValue("@perm", (int)permissions);
        cmd.Parameters.AddWithValue("@id", id);
        return cmd.ExecuteNonQuery() > 0;
    }

    /// <summary>
    /// Resets a local user's password: generates a fresh 16-byte salt and a new
    /// PBKDF2 hash. Scoped to <c>provider = 'local'</c>, so an OAuth account's id
    /// matches no rows and returns false (OAuth users have no password).
    /// </summary>
    public bool SetPassword(string id, string newPassword)
    {
        var salt = RandomNumberGenerator.GetBytes(16);
        var hash = HashPassword(newPassword, salt);

        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            UPDATE users SET password_hash = @h, salt = @s
            WHERE id = @id AND provider = 'local'
            """;
        cmd.Parameters.AddWithValue("@h",  hash);
        cmd.Parameters.AddWithValue("@s",  Convert.ToBase64String(salt));
        cmd.Parameters.AddWithValue("@id", id);
        return cmd.ExecuteNonQuery() > 0;
    }

    public string? GetRole(string username)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "SELECT role FROM users WHERE username = @u COLLATE NOCASE";
        cmd.Parameters.AddWithValue("@u", username);
        return cmd.ExecuteScalar() as string;
    }

    // ── OAuth domain allowlist ────────────────────────────────────────────────

    public IReadOnlyList<OAuthDomainRecord> ListOAuthDomains()
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "SELECT id, provider, domain, role, permissions, created_at FROM oauth_domains ORDER BY provider, domain";
        using var r    = cmd.ExecuteReader();
        var result = new List<OAuthDomainRecord>();
        while (r.Read())
            result.Add(new(r.GetString(0), r.GetString(1), r.GetString(2),
                           r.GetString(3), (ViewPermissions)r.GetInt32(4), DateTimeOffset.Parse(r.GetString(5))));
        return result;
    }

    public OAuthDomainRecord CreateOAuthDomain(string provider, string domain, string role,
        ViewPermissions permissions = ViewPermissions.All)
    {
        var rec = new OAuthDomainRecord(
            Guid.NewGuid().ToString("N"), provider, domain.ToLowerInvariant(),
            NormaliseRole(role), permissions, DateTimeOffset.UtcNow);

        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO oauth_domains (id, provider, domain, role, permissions, created_at)
            VALUES (@id, @p, @d, @r, @perm, @ca)
            """;
        cmd.Parameters.AddWithValue("@id", rec.Id);
        cmd.Parameters.AddWithValue("@p",  rec.Provider);
        cmd.Parameters.AddWithValue("@d",  rec.Domain);
        cmd.Parameters.AddWithValue("@r",  rec.Role);
        cmd.Parameters.AddWithValue("@perm", (int)rec.Permissions);
        cmd.Parameters.AddWithValue("@ca", rec.CreatedAt.ToString("O"));
        cmd.ExecuteNonQuery();
        return rec;
    }

    /// <summary>Updates an OAuth domain rule's default role + permissions.</summary>
    public bool UpdateOAuthDomain(string id, string role, ViewPermissions permissions)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "UPDATE oauth_domains SET role = @r, permissions = @perm WHERE id = @id";
        cmd.Parameters.AddWithValue("@id",   id);
        cmd.Parameters.AddWithValue("@r",    NormaliseRole(role));
        cmd.Parameters.AddWithValue("@perm", (int)permissions);
        return cmd.ExecuteNonQuery() > 0;
    }

    public bool DeleteOAuthDomain(string id)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM oauth_domains WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        return cmd.ExecuteNonQuery() > 0;
    }

    // ── API keys ──────────────────────────────────────────────────────────────

    public bool ValidateApiKey(string key)
    {
        var incoming = KeyHash(key);
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "SELECT key_hash FROM api_keys WHERE key_hash = @h";
        cmd.Parameters.AddWithValue("@h", incoming);
        var stored = cmd.ExecuteScalar() as string;
        if (stored is null) return false;
        return CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(incoming),
            Encoding.UTF8.GetBytes(stored));
    }

    public IReadOnlyList<ApiKeyRecord> ListApiKeys()
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "SELECT id, name, description, permissions, key_hash, created_by, created_at FROM api_keys ORDER BY created_at";
        using var r    = cmd.ExecuteReader();
        var result = new List<ApiKeyRecord>();
        while (r.Read())
            result.Add(new(r.GetString(0), r.GetString(1), r.GetString(2),
                           (ApiKeyPermissions)r.GetInt32(3), r.GetString(4), r.GetString(5),
                           DateTimeOffset.Parse(r.GetString(6))));
        return result;
    }

    public ApiKeyRecord CreateApiKey(
        string name, string description, ApiKeyPermissions permissions, string createdBy, string? manualKey = null)
    {
        // Auto-generated keys are a plain 48-char lowercase-hex token (no prefix);
        // a manual key is used verbatim.
        var key = manualKey?.Trim() is { Length: > 0 } mk
            ? mk
            : Convert.ToHexString(RandomNumberGenerator.GetBytes(24)).ToLowerInvariant();

        var hash = KeyHash(key);
        var rec  = new ApiKeyRecord(
            Guid.NewGuid().ToString("N"), name, description, permissions, hash,
            createdBy, DateTimeOffset.UtcNow)
        {
            Key = key,
        };

        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO api_keys (id, name, description, permissions, key_hash, created_by, created_at)
            VALUES (@id, @n, @desc, @perm, @h, @cb, @ca)
            """;
        cmd.Parameters.AddWithValue("@id",   rec.Id);
        cmd.Parameters.AddWithValue("@n",    rec.Name);
        cmd.Parameters.AddWithValue("@desc", rec.Description);
        cmd.Parameters.AddWithValue("@perm", (int)rec.Permissions);
        cmd.Parameters.AddWithValue("@h",    hash);
        cmd.Parameters.AddWithValue("@cb",   rec.CreatedBy);
        cmd.Parameters.AddWithValue("@ca",   rec.CreatedAt.ToString("O"));
        cmd.ExecuteNonQuery();
        return rec;
    }

    public bool DeleteApiKey(string id)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM api_keys WHERE id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        return cmd.ExecuteNonQuery() > 0;
    }

    /// <summary>Updates an API key's name, description and permission bits (not the secret).</summary>
    public bool UpdateApiKey(string id, string name, string description, ApiKeyPermissions permissions)
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "UPDATE api_keys SET name = @n, description = @desc, permissions = @perm WHERE id = @id";
        cmd.Parameters.AddWithValue("@id",   id);
        cmd.Parameters.AddWithValue("@n",    name);
        cmd.Parameters.AddWithValue("@desc", description);
        cmd.Parameters.AddWithValue("@perm", (int)permissions);
        return cmd.ExecuteNonQuery() > 0;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static UserRecord MapUser(SqliteDataReader r) => new(
        r.GetString(0), r.GetString(1), r.GetString(2),
        r.GetString(3), r.GetString(4), r.GetString(5),
        (ViewPermissions)r.GetInt32(7),
        DateTimeOffset.Parse(r.GetString(6)))
    {
        ProviderSubject = r.IsDBNull(8) ? "" : r.GetString(8),
    };

    private static string HashPassword(string password, byte[] salt)
    {
        var hash = Rfc2898DeriveBytes.Pbkdf2(password, salt, 200_000, HashAlgorithmName.SHA256, 32);
        return Convert.ToBase64String(hash);
    }

    private static string KeyHash(string key) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(key))).ToLowerInvariant();

    internal static string NormaliseRole(string role) =>
        role is "admin" or "manager" or "viewer" ? role : "viewer";

    private void EnsureSeedAdmin()
    {
        using var conn = _db.Open();
        using var cmd  = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM users WHERE provider = 'local'";
        var count = (long)(cmd.ExecuteScalar() ?? 0L);

        if (count == 0)
        {
            // Fresh install: never seed a well-known default. Use the configured
            // password if given, otherwise mint a random one and surface it once.
            bool generated = string.IsNullOrWhiteSpace(_opts.AdminPassword);
            string password = generated ? GenerateInitialPassword() : _opts.AdminPassword!;
            CreateUser("admin", password, "admin");

            if (generated)
                _logger.LogWarning(
                    "Seeded the initial 'admin' account with a RANDOM password: {Password}\n" +
                    "  Sign in and change it now (Settings → Users), or set Ameto:Auth:AdminPassword. Shown only once.",
                    password);
            else
                _logger.LogInformation("Seeded the initial 'admin' account from Ameto:Auth:AdminPassword.");
            return;
        }

        // Existing install: nudge operators still running the legacy default.
        if (ValidateUser("admin", "123123"))
            _logger.LogWarning(
                "The 'admin' account is still using the default password (123123). Change it in " +
                "Settings → Users — on a public network the server is otherwise trivially compromised.");
    }

    /// <summary>16-char URL-safe random password from a cryptographic RNG.</summary>
    private static string GenerateInitialPassword() =>
        Convert.ToBase64String(RandomNumberGenerator.GetBytes(12))
            .Replace('+', 'A').Replace('/', 'B').TrimEnd('=');
}
