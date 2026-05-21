import { Injectable, signal, computed, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Router } from '@angular/router';
import { tap } from 'rxjs';
import { LoginResponseDto } from '../models/auth.model';

const TOKEN_KEY   = 'rd_token';
const EXPIRES_KEY = 'rd_token_exp';
const ROLE_KEY    = 'rd_role';

@Injectable({ providedIn: 'root' })
export class AuthService {
  private http   = inject(HttpClient);
  private router = inject(Router);

  private _token     = signal<string | null>(localStorage.getItem(TOKEN_KEY));
  private _expiresAt = signal<number>(Number(localStorage.getItem(EXPIRES_KEY) ?? 0));
  private _role      = signal<string>(localStorage.getItem(ROLE_KEY) ?? 'manager');

  isAuthenticated = computed(() => !!this._token() && this._expiresAt() > Date.now());
  isAdmin         = computed(() => this._role() === 'admin');

  constructor() {
    setInterval(() => this.autoRefresh(), 60_000);
  }

  get token(): string | null { return this._token(); }
  get role(): string         { return this._role(); }

  login(username: string, password: string) {
    return this.http
      .post<LoginResponseDto>('/api/auth/login', { username, password })
      .pipe(tap(r => this.storeToken(r)));
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
    this._role.set('manager');
    this.router.navigate(['/login']);
  }

  private storeToken(r: LoginResponseDto): void {
    // Decode role from JWT payload (second base64 segment)
    let role = 'manager';
    try {
      const payload = JSON.parse(atob(r.token.split('.')[1]));
      role = payload['http://schemas.microsoft.com/ws/2008/06/identity/claims/role']
          ?? payload['role']
          ?? 'manager';
    } catch { /* ignore */ }

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
