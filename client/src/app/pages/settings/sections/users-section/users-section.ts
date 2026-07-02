import {
  ChangeDetectionStrategy,
  Component,
  computed,
  effect,
  inject,
  signal,
  OnInit,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { DatePipe } from '@angular/common';
import { Router } from '@angular/router';
import { LucideAngularModule } from 'lucide-angular';
import { SectionComponent } from '../../../../shared/components/ui';
import { ApiService } from '../../../../core/services/api.service';
import { AuthService } from '../../../../core/services/auth.service';
import { UserDto, UserRole, UserProvider, OAuthDomainDto } from '../../../../core/models/auth.model';
import { SettingsDirtyService } from '../../settings-dirty.service';

type AddTab = 'local' | 'oauth-email' | 'oauth-domain';

@Component({
  selector: 'app-users-section',
  imports: [LucideAngularModule, FormsModule, DatePipe, SectionComponent],
  templateUrl: './users-section.html',
  styleUrl: './users-section.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class UsersSectionComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly dirtyService = inject(SettingsDirtyService);
  private readonly router = inject(Router);
  readonly authService = inject(AuthService);

  readonly users = signal<UserDto[]>([]);
  readonly loading = signal(false);
  readonly userError = signal<string | null>(null);
  readonly oauthUserError = signal<string | null>(null);
  readonly domainError = signal<string | null>(null);

  readonly domains = signal<OAuthDomainDto[]>([]);
  readonly domainsLoading = signal(false);

  readonly addUserTab = signal<AddTab>('local');

  readonly newUsername = signal('');
  readonly newPassword = signal('');
  readonly newRole = signal<UserRole>('viewer');

  readonly newOAuthEmail = signal('');
  readonly newOAuthDisplay = signal('');
  readonly newOAuthProvider = signal<UserProvider>('google');
  readonly newOAuthRole = signal<UserRole>('viewer');

  readonly newDomain = signal('');
  readonly newDomainProvider = signal<UserProvider>('google');
  readonly newDomainRole = signal<UserRole>('viewer');

  readonly dirty = computed(
    () =>
      this.newUsername().trim().length > 0 ||
      this.newPassword().trim().length > 0 ||
      this.newOAuthEmail().trim().length > 0 ||
      this.newOAuthDisplay().trim().length > 0 ||
      this.newDomain().trim().length > 0,
  );

  constructor() {
    effect(() => this.dirtyService.mark('users', this.dirty()));
  }

  ngOnInit(): void {
    this.loadUsers();
    this.loadDomains();
  }

  openUser(id: string): void {
    this.router.navigate(['/settings/users', id]);
  }

  private loadUsers(): void {
    this.loading.set(true);
    this.api.getUsers().subscribe({
      next: u => { this.users.set(u); this.loading.set(false); },
      error: () => this.loading.set(false),
    });
  }

  private loadDomains(): void {
    this.domainsLoading.set(true);
    this.api.getOAuthDomains().subscribe({
      next: d => { this.domains.set(d); this.domainsLoading.set(false); },
      error: () => this.domainsLoading.set(false),
    });
  }

  addUser(): void {
    const username = this.newUsername().trim();
    const password = this.newPassword().trim();
    if (!username || !password) {
      this.userError.set('Username and password are required.');
      return;
    }
    this.userError.set(null);
    this.api.createUser(username, password, this.newRole()).subscribe({
      next: u => {
        this.users.update(arr => [...arr, u]);
        this.newUsername.set('');
        this.newPassword.set('');
        this.newRole.set('viewer');
      },
      error: () => this.userError.set('Failed to create user.'),
    });
  }

  addOAuthUser(): void {
    const email = this.newOAuthEmail().trim();
    if (!email) {
      this.oauthUserError.set('Email is required.');
      return;
    }
    this.oauthUserError.set(null);
    this.api.createOAuthUser(
      email,
      this.newOAuthDisplay().trim() || email,
      this.newOAuthProvider(),
      this.newOAuthRole(),
    ).subscribe({
      next: u => {
        this.users.update(arr => [...arr, u]);
        this.newOAuthEmail.set('');
        this.newOAuthDisplay.set('');
        this.newOAuthProvider.set('google');
        this.newOAuthRole.set('viewer');
      },
      error: () => this.oauthUserError.set('Failed to add OAuth user. Email may already exist.'),
    });
  }

  addDomain(): void {
    const domain = this.newDomain().trim().replace(/^@/, '');
    if (!domain) {
      this.domainError.set('Domain is required (e.g. ameto.com).');
      return;
    }
    this.domainError.set(null);
    this.api.createOAuthDomain(domain, this.newDomainProvider(), this.newDomainRole()).subscribe({
      next: d => {
        this.domains.update(arr => [...arr, d]);
        this.newDomain.set('');
        this.newDomainRole.set('viewer');
      },
      error: () => this.domainError.set('Failed to add domain. It may already exist for this provider.'),
    });
  }

  removeDomain(id: string): void {
    this.api.deleteOAuthDomain(id).subscribe({
      next: () => this.domains.update(arr => arr.filter(d => d.id !== id)),
    });
  }
}
