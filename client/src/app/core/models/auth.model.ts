export type UserRole = 'admin' | 'manager' | 'viewer';
export type UserProvider = 'local' | 'google' | 'microsoft';

/** Per-user read scopes (mirror of the server's ViewPermissions bit flags). */
export const enum ViewPermission {
  Logs    = 1,
  Metrics = 2,
  Traces  = 4,
  Stats   = 8,
}

/** Every scope granted — the value stored for admins and unrestricted users. */
export const ALL_VIEW_PERMISSIONS =
  ViewPermission.Logs | ViewPermission.Metrics | ViewPermission.Traces | ViewPermission.Stats;

export interface UserDto {
  id: string;
  username: string;
  displayName: string;
  email: string;
  provider: UserProvider;
  role: UserRole;
  permissions: number;
  createdAt: string;
}

/** API-key permission bit flags (mirror of the server's ApiKeyPermissions).
 *  Low three = ingest (write); the Read* bits = query (read) per signal. */
export const enum ApiKeyPermission {
  Logs        = 1,
  Traces      = 2,
  Metrics     = 4,
  ReadLogs    = 8,
  ReadTraces  = 16,
  ReadMetrics = 32,
}

export interface ApiKeyDto {
  id: string;
  name: string;
  description: string;
  permissions: number;
  keyPreview: string;
  createdBy: string;
  createdAt: string;
}

export interface CreatedApiKeyDto {
  id: string;
  name: string;
  description: string;
  permissions: number;
  key: string;
  createdBy: string;
  createdAt: string;
}

export interface OAuthDomainDto {
  id: string;
  provider: 'google' | 'microsoft';
  domain: string;
  role: UserRole;
  /** Default view scopes granted to users auto-provisioned by this rule. */
  permissions: number;
  createdAt: string;
}

export interface LoginResponseDto {
  token: string;
  expiresIn: number;
  role: UserRole;
  /** Effective view-scope bitmask (admin → all). Falls back to the JWT `perm` claim. */
  permissions?: number;
}

export interface AuthProvidersDto {
  local: boolean;
  google: boolean;
  microsoft: boolean;
}
