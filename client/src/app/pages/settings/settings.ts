import { Component, signal, inject, OnInit, ChangeDetectionStrategy, ChangeDetectorRef } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { DatePipe } from '@angular/common';
import { RouterLink } from '@angular/router';
import { LucideAngularModule } from 'lucide-angular';
import { ThemeService } from '../../core/services/theme.service';
import { ApiService } from '../../core/services/api.service';
import { AuthService } from '../../core/services/auth.service';
import { UserPreferencesService } from '../../core/services/user-preferences.service';
import { RetentionDto, RetentionRunResult } from '../../core/models/retention.model';
import { UserDto, ApiKeyDto, CreatedApiKeyDto } from '../../core/models/auth.model';
import { PageHeaderComponent, SectionComponent } from '../../shared/components/ui';

@Component({
  selector: 'app-settings',
  imports: [LucideAngularModule, FormsModule, DatePipe, RouterLink, PageHeaderComponent, SectionComponent],
  templateUrl: './settings.html',
  styleUrl: './settings.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class SettingsComponent implements OnInit {
  theme       = inject(ThemeService);
  authService = inject(AuthService);
  prefs       = inject(UserPreferencesService);
  private api = inject(ApiService);
  private cdr = inject(ChangeDetectorRef);

  overviewCustomPropsCsv = '';

  retentionLoading = signal(false);
  retentionSaving  = signal(false);
  retentionRunning = signal(false);
  runResult        = signal<RetentionRunResult | null>(null);

  verboseDays     = 3;
  debugDays       = 3;
  informationDays = 90;
  warningDays     = 90;
  errorDays       = 90;
  fatalDays       = 90;
  metricsDays     = 30;
  tracesDays      = 14;

  // ── Users ──────────────────────────────────────────────────────────────────
  users       = signal<UserDto[]>([]);
  usersLoading = signal(false);
  newUsername = '';
  newPassword = '';
  newRole: 'admin' | 'manager' | 'viewer' = 'viewer';
  userError = signal<string | null>(null);

  // OAuth user form
  newOAuthEmail    = '';
  newOAuthDisplay  = '';
  newOAuthProvider: 'google' | 'microsoft' = 'google';
  newOAuthRole: 'admin' | 'manager' | 'viewer' = 'viewer';
  oauthUserError = signal<string | null>(null);
  addUserTab: 'local' | 'oauth' = 'local';

  // ── API Keys ───────────────────────────────────────────────────────────────
  apiKeys       = signal<ApiKeyDto[]>([]);
  keysLoading   = signal(false);
  newKeyName    = '';
  newKeyManual  = '';
  keyError      = signal<string | null>(null);
  createdKey    = signal<CreatedApiKeyDto | null>(null);

  ngOnInit(): void {
    this.overviewCustomPropsCsv = this.prefs.overviewCustomPropsCsv();
    this.retentionLoading.set(true);
    this.api.getRetention().subscribe({
      next: r => {
        this.verboseDays     = r.verboseDays;
        this.debugDays       = r.debugDays;
        this.informationDays = r.informationDays;
        this.warningDays     = r.warningDays;
        this.errorDays       = r.errorDays;
        this.fatalDays       = r.fatalDays;
        this.metricsDays     = r.metricsDays;
        this.tracesDays      = r.tracesDays;
        this.retentionLoading.set(false);
        this.cdr.markForCheck();
      },
      error: () => { this.retentionLoading.set(false); this.cdr.markForCheck(); },
    });

    if (this.authService.isAuthenticated()) {
      this.loadApiKeys();
      if (this.authService.isAdmin()) this.loadUsers();
    }
  }

  saveRetention(): void {
    const dto: RetentionDto = {
      verboseDays:     this.verboseDays,
      debugDays:       this.debugDays,
      informationDays: this.informationDays,
      warningDays:     this.warningDays,
      errorDays:       this.errorDays,
      fatalDays:       this.fatalDays,
      metricsDays:     this.metricsDays,
      tracesDays:      this.tracesDays,
    };
    this.retentionSaving.set(true);
    this.api.putRetention(dto).subscribe({
      next: () => { this.retentionSaving.set(false); this.cdr.markForCheck(); },
      error: () => { this.retentionSaving.set(false); this.cdr.markForCheck(); },
    });
  }

  runRetention(): void {
    this.retentionRunning.set(true);
    this.runResult.set(null);
    this.api.runRetention().subscribe({
      next: r => { this.runResult.set(r); this.retentionRunning.set(false); this.cdr.markForCheck(); },
      error: () => { this.retentionRunning.set(false); this.cdr.markForCheck(); },
    });
  }

  // ── Users ──────────────────────────────────────────────────────────────────
  loadUsers(): void {
    this.usersLoading.set(true);
    this.api.getUsers().subscribe({
      next: u => { this.users.set(u); this.usersLoading.set(false); this.cdr.markForCheck(); },
      error: () => { this.usersLoading.set(false); this.cdr.markForCheck(); },
    });
  }

  addUser(): void {
    if (!this.newUsername.trim() || !this.newPassword.trim()) {
      this.userError.set('Username and password are required.'); return;
    }
    this.userError.set(null);
    this.api.createUser(this.newUsername.trim(), this.newPassword.trim(), this.newRole).subscribe({
      next: u => {
        this.users.update(arr => [...arr, u]);
        this.newUsername = ''; this.newPassword = ''; this.newRole = 'viewer';
        this.cdr.markForCheck();
      },
      error: () => { this.userError.set('Failed to create user.'); this.cdr.markForCheck(); },
    });
  }

  addOAuthUser(): void {
    if (!this.newOAuthEmail.trim()) {
      this.oauthUserError.set('Email is required.'); return;
    }
    this.oauthUserError.set(null);
    this.api.createOAuthUser(
      this.newOAuthEmail.trim(),
      this.newOAuthDisplay.trim() || this.newOAuthEmail.trim(),
      this.newOAuthProvider,
      this.newOAuthRole,
    ).subscribe({
      next: u => {
        this.users.update(arr => [...arr, u]);
        this.newOAuthEmail = ''; this.newOAuthDisplay = '';
        this.newOAuthProvider = 'google'; this.newOAuthRole = 'viewer';
        this.cdr.markForCheck();
      },
      error: () => { this.oauthUserError.set('Failed to add OAuth user. Email may already exist.'); this.cdr.markForCheck(); },
    });
  }

  changeRole(id: string, role: string): void {
    this.api.updateUserRole(id, role).subscribe({
      next: () => {
        this.users.update(arr => arr.map(u => u.id === id ? { ...u, role: role as any } : u));
        this.cdr.markForCheck();
      },
    });
  }

  removeUser(id: string): void {
    this.api.deleteUser(id).subscribe({
      next: () => { this.users.update(arr => arr.filter(u => u.id !== id)); this.cdr.markForCheck(); },
    });
  }

  // ── API Keys ───────────────────────────────────────────────────────────────
  loadApiKeys(): void {
    this.keysLoading.set(true);
    this.api.getApiKeys().subscribe({
      next: k => { this.apiKeys.set(k); this.keysLoading.set(false); this.cdr.markForCheck(); },
      error: () => { this.keysLoading.set(false); this.cdr.markForCheck(); },
    });
  }

  addApiKey(): void {
    if (!this.newKeyName.trim()) { this.keyError.set('Name is required.'); return; }
    this.keyError.set(null);
    this.createdKey.set(null);
    this.api.createApiKey(this.newKeyName.trim(), this.newKeyManual.trim() || undefined).subscribe({
      next: ck => {
        this.createdKey.set(ck);
        this.apiKeys.update(arr => [...arr, { id: ck.id, name: ck.name, keyPreview: ck.key.slice(0, 12) + '…', createdBy: ck.createdBy, createdAt: ck.createdAt }]);
        this.newKeyName = ''; this.newKeyManual = '';
        this.cdr.markForCheck();
      },
      error: () => { this.keyError.set('Failed to create API key.'); this.cdr.markForCheck(); },
    });
  }

  removeApiKey(id: string): void {
    this.api.deleteApiKey(id).subscribe({
      next: () => { this.apiKeys.update(arr => arr.filter(k => k.id !== id)); this.cdr.markForCheck(); },
    });
  }

  dismissCreatedKey(): void { this.createdKey.set(null); this.cdr.markForCheck(); }

  saveOverviewPrefs(): void {
    this.prefs.setOverviewCustomPropsFromCsv(this.overviewCustomPropsCsv);
    this.overviewCustomPropsCsv = this.prefs.overviewCustomPropsCsv();
    this.cdr.markForCheck();
  }
}

