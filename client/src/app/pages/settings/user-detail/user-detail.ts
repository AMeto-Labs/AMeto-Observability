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
import { UserDto, UserRole } from '../../../core/models/auth.model';
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
  readonly saving = signal(false);
  readonly deleting = signal(false);

  readonly userId = signal('');

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
    this.api.updateUser(id, name, this.editRole()).subscribe({
      next: () => {
        this.user.update(u => (u ? { ...u, displayName: name, role: this.editRole() } : u));
        this.editing.set(false);
        this.saving.set(false);
      },
      error: () => { this.saving.set(false); },
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
