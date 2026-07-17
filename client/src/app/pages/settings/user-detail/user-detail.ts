import {
  ChangeDetectionStrategy,
  Component,
  OnInit,
  computed,
  inject,
  signal,
} from '@angular/core';
import { DatePipe } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { LucideAngularModule } from 'lucide-angular';
import { ApiService } from '../../../core/services/api.service';
import { AuthService } from '../../../core/services/auth.service';
import {
  ALL_VIEW_PERMISSIONS, UserDto, UserRole, ViewPermission,
} from '../../../core/models/auth.model';
import { PageHeaderComponent, SectionComponent } from '../../../shared/components/ui';

@Component({
  selector: 'app-user-detail',
  imports: [LucideAngularModule, FormsModule, DatePipe, PageHeaderComponent, SectionComponent],
  templateUrl: './user-detail.html',
  styleUrl: './user-detail.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class UserDetailComponent implements OnInit {
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly api = inject(ApiService);
  readonly authService = inject(AuthService);

  readonly user = signal<UserDto | null>(null);
  readonly loading = signal(true);
  readonly error = signal<string | null>(null);

  readonly editing = signal(false);
  readonly editDisplayName = signal('');
  readonly editRole = signal<UserRole>('viewer');
  readonly editPermissions = signal<number>(ALL_VIEW_PERMISSIONS);
  readonly saving = signal(false);
  readonly deleting = signal(false);

  // ── Password reset (local users only) ─────────────────────────────────────
  readonly changingPassword = signal(false);
  readonly newPassword = signal('');
  readonly confirmPassword = signal('');
  readonly pwSaving = signal(false);
  readonly pwError = signal<string | null>(null);
  readonly pwSuccess = signal(false);

  /** Only local-provider accounts have a password to reset. */
  readonly isLocal = computed(() => this.user()?.provider === 'local');

  readonly userId = signal('');

  /** View scopes offered in the editor (admins are always granted all). */
  readonly permScopes: { bit: ViewPermission; label: string }[] = [
    { bit: ViewPermission.Logs,    label: 'Logs'    },
    { bit: ViewPermission.Metrics, label: 'Metrics' },
    { bit: ViewPermission.Traces,  label: 'Traces'  },
    { bit: ViewPermission.Stats,   label: 'Stats'   },
  ];

  hasPerm(bit: ViewPermission): boolean {
    return (this.editPermissions() & bit) === bit;
  }

  togglePerm(bit: ViewPermission): void {
    this.editPermissions.update(p => (p & bit) === bit ? p & ~bit : p | bit);
  }

  /** Comma-separated scope labels for the read-only profile row (admin → "All"). */
  permsLabel(role: UserRole, permissions: number): string {
    if (role === 'admin') return 'All (admin)';
    const on = this.permScopes.filter(s => (permissions & s.bit) === s.bit).map(s => s.label);
    return on.length ? on.join(', ') : 'None';
  }

  readonly isSelf = computed(() => {
    // The JWT Name claim holds the username; AuthService exposes the raw token.
    // Self-protection is enforced server-side, so this is only for UX hints.
    return false;
  });

  ngOnInit(): void {
    const id = this.route.snapshot.paramMap.get('id') ?? '';
    this.userId.set(id);
    this.loadUser(id);
  }

  private loadUser(id: string): void {
    this.loading.set(true);
    this.api.getUser(id).subscribe({
      next: u => { this.user.set(u); this.loading.set(false); },
      error: () => { this.error.set('User not found.'); this.loading.set(false); },
    });
  }

  startEdit(): void {
    const u = this.user();
    if (!u) return;
    this.editDisplayName.set(u.displayName);
    this.editRole.set(u.role);
    this.editPermissions.set(u.permissions ?? ALL_VIEW_PERMISSIONS);
    this.editing.set(true);
  }

  cancelEdit(): void {
    this.editing.set(false);
  }

  save(): void {
    const id = this.userId();
    const name = this.editDisplayName().trim();
    if (!name) return;
    this.saving.set(true);
    // Admins always hold every scope; persist All so the stored value is unambiguous.
    const perms = this.editRole() === 'admin' ? ALL_VIEW_PERMISSIONS : this.editPermissions();
    this.api.updateUser(id, name, this.editRole(), perms).subscribe({
      next: () => {
        this.user.update(u => (u ? { ...u, displayName: name, role: this.editRole(), permissions: perms } : u));
        this.editing.set(false);
        this.saving.set(false);
      },
      error: () => { this.saving.set(false); },
    });
  }

  // ── Password reset ────────────────────────────────────────────────────────
  startPasswordChange(): void {
    this.newPassword.set('');
    this.confirmPassword.set('');
    this.pwError.set(null);
    this.pwSuccess.set(false);
    this.changingPassword.set(true);
  }

  cancelPasswordChange(): void {
    this.changingPassword.set(false);
    this.pwError.set(null);
  }

  savePassword(): void {
    const pw = this.newPassword();
    this.pwSuccess.set(false);
    if (pw.length < 6) {
      this.pwError.set('Password must be at least 6 characters.');
      return;
    }
    if (pw !== this.confirmPassword()) {
      this.pwError.set('Passwords do not match.');
      return;
    }
    this.pwError.set(null);
    this.pwSaving.set(true);
    this.api.changeUserPassword(this.userId(), pw).subscribe({
      next: () => {
        this.pwSaving.set(false);
        this.changingPassword.set(false);
        this.newPassword.set('');
        this.confirmPassword.set('');
        this.pwSuccess.set(true);
      },
      error: (e) => {
        this.pwSaving.set(false);
        this.pwError.set(e?.error?.error ?? 'Failed to change password.');
      },
    });
  }

  remove(): void {
    const u = this.user();
    if (!u) return;
    if (!confirm(`Delete user "${u.displayName || u.email || u.username}"? This cannot be undone.`)) return;
    this.deleting.set(true);
    this.api.deleteUser(this.userId()).subscribe({
      next: () => { this.router.navigate(['/settings'], { queryParams: { tab: 'users' } }); },
      error: () => { this.deleting.set(false); },
    });
  }

  back(): void {
    this.router.navigate(['/settings'], { queryParams: { tab: 'users' } });
  }
}
