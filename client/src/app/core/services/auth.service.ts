import { Injectable, signal, computed, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Router } from '@angular/router';
import { tap } from 'rxjs';
import { AuthProvidersDto, LoginResponseDto, UserRole } from '../models/auth.model';

const TOKEN_KEY   = 'rd_token';
const EXPIRES_KEY = 'rd_token_exp';
const ROLE_KEY    = 'rd_role';
const RETURN_KEY  = 'rd_return_url';

@Injectable({ providedIn: 'root' })
export class AuthService {
  private http   = inject(HttpClient);
  private router = inject(Router);

  private _token     = signal<string | null>(localStorage.getItem(TOKEN_KEY));
  private _expiresAt = signal<number>(Number(localStorage.getItem(EXPIRES_KEY) ?? 0));
  private _role      = signal<UserRole>((localStorage.getItem(ROLE_KEY) as UserRole) ?? 'viewer');

  isAuthenticated = computed(() => !!this._token() && this._expiresAt() > Date.now());
  isAdmin         = computed(() => this._role() === 'admin');
  isManager       = computed(() => this._role() === 'admin' || this._role() === 'manager');

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
    window.location.href = `/api/auth/oauth/${provider}`;
  }

  /** Called by the OAuth callback route — stores the token from the URL fragment. */
  handleOAuthCallback(token: string, expiresIn: number, role: string): void {
    this.storeToken({ token, expiresIn, role: role as UserRole });
    const returnUrl = sessionStorage.getItem(RETURN_KEY);
    sessionStorage.removeItem(RETURN_KEY);
    this.router.navigateByUrl(returnUrl || '/events');
  }

  getProviders() {
    return this.http.get<AuthProvidersDto>('/api/auth/providers');
  }

  refresh() {
    return this.http
      .post<LoginResponseDto>('/api/auth/refresh', null)
      .pipe(tap(r => this.storeToken(r)));
  }

  logout(): void {
    [TOKEN_KEY, EXPIRES_KEY, ROLE_KEY].forEach(k => localStorage.removeItem(k));
    this._token.set(null);
    this._expiresAt.set(0);
    this._role.set('viewer');
    this.router.navigate(['/login']);
  }

  private storeToken(r: LoginResponseDto): void {
    // Prefer role from response body; fall back to JWT payload decode
    let role: UserRole = r.role ?? 'viewer';
    if (!role) {
      try {
        const payload = JSON.parse(atob(r.token.split('.')[1]));
        role = (payload['http://schemas.microsoft.com/ws/2008/06/identity/claims/role']
             ?? payload['role']
             ?? 'viewer') as UserRole;
      } catch { /* ignore */ }
    }

    const expiresAt = Date.now() + r.expiresIn * 1000;
    localStorage.setItem(TOKEN_KEY,   r.token);
    localStorage.setItem(EXPIRES_KEY, String(expiresAt));
    localStorage.setItem(ROLE_KEY,    role);
    this._token.set(r.token);
    this._expiresAt.set(expiresAt);
    this._role.set(role);
  }

  private autoRefresh(): void {
    if (!this.isAuthenticated()) return;
    if (this._expiresAt() - Date.now() < 5 * 60 * 1000)
      this.refresh().subscribe({ error: () => this.logout() });
  }
}

