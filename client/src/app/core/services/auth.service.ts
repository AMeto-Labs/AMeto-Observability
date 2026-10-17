import { Injectable, signal, computed, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Router } from '@angular/router';
import { Observable, finalize, shareReplay, tap } from 'rxjs';
import { appPath } from '../../shared/utils/app-url';
import {
  ALL_VIEW_PERMISSIONS, AuthProvidersDto, LoginResponseDto, UserRole, ViewPermission,
} from '../models/auth.model';

const TOKEN_KEY   = 'rd_token';
const EXPIRES_KEY = 'rd_token_exp';
const ROLE_KEY    = 'rd_role';
const PERMS_KEY   = 'rd_perms';
const RETURN_KEY  = 'rd_return_url';

/** Refresh this long before the access token runs out. It lives 15 minutes. */
const REFRESH_AHEAD_MS = 3 * 60_000;

/** True for an HTTP 401: the server saying there is no session to continue. */
export function isUnauthorized(err: unknown): boolean {
  return (err as { status?: number } | null)?.status === 401;
}

/**
 * The sign-in, as the page holds it.
 *
 * <p>Two credentials, kept in two places. The access token — 15 minutes — is in localStorage
 * and goes out as a bearer header. The session that outlives it is an HttpOnly cookie the page
 * cannot read: `/api/auth/refresh` trades it for a new access token and a rotated cookie. So an
 * expired token is not a signed-out user; it is a refresh away from being signed in again, and
 * only a 401 from that refresh means the session is over (ended, blocked, or a month unused).</p>
 */
@Injectable({ providedIn: 'root' })
export class AuthService {
  private http   = inject(HttpClient);
  private router = inject(Router);

  private _token     = signal<string | null>(localStorage.getItem(TOKEN_KEY));
  private _expiresAt = signal<number>(Number(localStorage.getItem(EXPIRES_KEY) ?? 0));
  private _role      = signal<UserRole>((localStorage.getItem(ROLE_KEY) as UserRole) ?? 'viewer');
  private _perms     = signal<number>(Number(localStorage.getItem(PERMS_KEY) ?? ALL_VIEW_PERMISSIONS));

  /** The refresh in flight, shared by everyone who needs a token while it runs. */
  private refreshing: Observable<LoginResponseDto> | null = null;

  /**
   * True while this browser is signed in as far as the page can tell: a token was issued and no
   * sign-out or refused refresh has cleared it since. The token may have expired meanwhile.
   */
  hasSession  = computed(() => !!this._token());
  isAdmin     = computed(() => this._role() === 'admin');
  isManager   = computed(() => this._role() === 'admin' || this._role() === 'manager');
  /** Effective view-scope bitmask for the signed-in user (admin → all). */
  permissions = computed(() => (this._role() === 'admin' ? ALL_VIEW_PERMISSIONS : this._perms()));

  /**
   * True when the access token is usable right now. A method, not a computed: the answer changes
   * with the clock, and a computed would keep saying yes after expiry until a signal moved.
   */
  isAuthenticated(): boolean {
    return !!this._token() && this._expiresAt() > Date.now();
  }

  /** True when the user may view the given scope. Admin always passes. */
  can(scope: ViewPermission): boolean {
    return (this.permissions() & scope) === scope;
  }

  constructor() {
    setInterval(() => this.autoRefresh(), 60_000);
  }

  get token(): string | null { return this._token(); }
  get role(): UserRole       { return this._role(); }

  login(username: string, password: string) {
    return this.http
      .post<LoginResponseDto>('/api/auth/login', { username, password })
      .pipe(tap(r => this.storeToken(r)));
  }

  /** Redirects browser to OAuth provider (full page redirect, not XHR). */
  loginWithOAuth(provider: 'google' | 'microsoft', returnUrl?: string): void {
    // Full-page redirect drops component state, so persist the return target.
    if (returnUrl) sessionStorage.setItem(RETURN_KEY, returnUrl);
    else sessionStorage.removeItem(RETURN_KEY);
    // Full-page navigation, so it bypasses the HttpClient interceptor that would
    // otherwise apply the deployment prefix — resolve it here instead.
    window.location.href = appPath(`/api/auth/oauth/${provider}`);
  }

  /**
   * Called by the OAuth callback route. The server has set the session cookie on the redirect
   * that brought the browser here — and put no token in the URL — so the token comes from a
   * refresh. Resolves to where the user should go next.
   */
  finishOAuthSignIn(): Observable<LoginResponseDto> {
    return this.refresh();
  }

  /** Where to go after an OAuth sign-in: the page the user started from, once. */
  takeOAuthReturnUrl(): string {
    const returnUrl = sessionStorage.getItem(RETURN_KEY);
    sessionStorage.removeItem(RETURN_KEY);
    return returnUrl || '/events';
  }

  getProviders() {
    return this.http.get<AuthProvidersDto>('/api/auth/providers');
  }

  /**
   * Trades the session cookie for a new access token. Concurrent callers — a burst of requests
   * that all came back 401 when the token ran out — share one request: each refresh rotates the
   * cookie, and a second one racing the first would only be answered from the server's grace.
   * A 401 clears the session; any other failure leaves it alone for the next attempt.
   */
  refresh(): Observable<LoginResponseDto> {
    this.refreshing ??= this.http.post<LoginResponseDto>('/api/auth/refresh', null).pipe(
      tap({
        next:  r   => this.storeToken(r),
        error: err => { if (isUnauthorized(err)) this.clearSession(); },
      }),
      finalize(() => { this.refreshing = null; }),
      shareReplay({ bufferSize: 1, refCount: false }),
    );
    return this.refreshing;
  }

  /** Signs out here and on the server, which ends the session and clears its cookie. */
  logout(): void {
    if (this.hasSession())
      this.http.post('/api/auth/logout', null).subscribe({ error: () => { /* signed out locally regardless */ } });
    this.clearSession();
    this.router.navigate(['/login']);
  }

  /**
   * Re-validates the session after a channel that bypasses the HTTP interceptor fails.
   * `EventSource` can neither send the `Authorization` header nor expose the response
   * status, so an SSE 401 (an expired token, an ended session) never reaches
   * {@link authInterceptor}. A refresh answers the question: a 401 there means the session is
   * gone and signs out, while a transient/network error leaves the session intact (no false
   * logout on a blip).
   */
  verifySession(): void {
    if (!this.hasSession()) { this.logout(); return; }
    this.refresh().subscribe({ error: err => { if (isUnauthorized(err)) this.logout(); } });
  }

  private clearSession(): void {
    [TOKEN_KEY, EXPIRES_KEY, ROLE_KEY, PERMS_KEY].forEach(k => localStorage.removeItem(k));
    this._token.set(null);
    this._expiresAt.set(0);
    this._role.set('viewer');
    this._perms.set(ALL_VIEW_PERMISSIONS);
  }

  private storeToken(r: LoginResponseDto): void {
    // Prefer role from response body; fall back to JWT payload decode
    let role: UserRole = r.role ?? 'viewer';
    let claimPerms: number | undefined;
    if (!role || r.permissions === undefined) {
      try {
        const payload = JSON.parse(atob(r.token.split('.')[1]));
        role ||= (payload['http://schemas.microsoft.com/ws/2008/06/identity/claims/role']
             ?? payload['role']
             ?? 'viewer') as UserRole;
        const p = Number(payload['perm']);
        if (Number.isFinite(p)) claimPerms = p;
      } catch { /* ignore */ }
    }

    // Effective scope: admins get everything; otherwise take the response value,
    // then the JWT `perm` claim, then a safe all-access fallback.
    const perms = role === 'admin'
      ? ALL_VIEW_PERMISSIONS
      : (r.permissions ?? claimPerms ?? ALL_VIEW_PERMISSIONS);

    const expiresAt = Date.now() + r.expiresIn * 1000;
    localStorage.setItem(TOKEN_KEY,   r.token);
    localStorage.setItem(EXPIRES_KEY, String(expiresAt));
    localStorage.setItem(ROLE_KEY,    role);
    localStorage.setItem(PERMS_KEY,   String(perms));
    this._token.set(r.token);
    this._expiresAt.set(expiresAt);
    this._role.set(role);
    this._perms.set(perms);
  }

  /**
   * Keeps the token fresh while the page is open, and renews an expired one as soon as the page
   * runs again — after the laptop wakes, say. A 401 signs out; a network error waits for the
   * next tick.
   */
  private autoRefresh(): void {
    if (!this.hasSession()) return;
    if (this._expiresAt() - Date.now() < REFRESH_AHEAD_MS)
      this.refresh().subscribe({ error: err => { if (isUnauthorized(err)) this.logout(); } });
  }
}
