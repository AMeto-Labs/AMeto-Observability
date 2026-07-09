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
import { LucideAngularModule } from 'lucide-angular';
import { SectionComponent, ModalComponent } from '../../../../shared/components/ui';
import { ApiService } from '../../../../core/services/api.service';
import { ApiKeyDto, CreatedApiKeyDto, ApiKeyPermission } from '../../../../core/models/auth.model';
import { SettingsDirtyService } from '../../settings-dirty.service';

/** Bit set covering both trace + metric ingest, presented as one choice in the UI. */
const TRACES_METRICS = ApiKeyPermission.Traces | ApiKeyPermission.Metrics;

@Component({
  selector: 'app-api-keys-section',
  imports: [LucideAngularModule, FormsModule, DatePipe, SectionComponent, ModalComponent],
  templateUrl: './api-keys-section.html',
  styleUrl: './api-keys-section.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ApiKeysSectionComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly dirtyService = inject(SettingsDirtyService);

  readonly apiKeys = signal<ApiKeyDto[]>([]);
  readonly loading = signal(false);
  readonly keyError = signal<string | null>(null);
  readonly createdKey = signal<CreatedApiKeyDto | null>(null);
  readonly copied = signal(false);

  /** Modal + create form state. */
  readonly modalOpen = signal(false);
  readonly creating = signal(false);
  readonly newName = signal('');
  readonly newDescription = signal('');
  readonly newKeyManual = signal('');

  /** Ingest scopes granted to the new key (both on by default). */
  readonly permLogs = signal(true);
  readonly permTracesMetrics = signal(true);

  /** Combined permission bit-set sent to the server; ≥ 1 scope required. */
  readonly newPermissions = computed(
    () => (this.permLogs() ? ApiKeyPermission.Logs : 0) | (this.permTracesMetrics() ? TRACES_METRICS : 0),
  );

  /** Dirty while the create form has input. */
  readonly dirty = computed(
    () => this.newName().trim().length > 0 || this.newDescription().trim().length > 0 || this.newKeyManual().trim().length > 0,
  );

  constructor() {
    effect(() => this.dirtyService.mark('api-keys', this.dirty()));
  }

  ngOnInit(): void {
    this.loadApiKeys();
  }

  private loadApiKeys(): void {
    this.loading.set(true);
    this.api.getApiKeys().subscribe({
      next: k => { this.apiKeys.set(k); this.loading.set(false); },
      error: () => this.loading.set(false),
    });
  }

  openCreateModal(): void {
    this.resetForm();
    this.createdKey.set(null);
    this.keyError.set(null);
    this.modalOpen.set(true);
  }

  closeCreateModal(): void {
    // Block closing while a key was just created (must be acknowledged) — the
    // created-key view has its own Done button.
    if (this.createdKey()) return;
    this.modalOpen.set(false);
    this.resetForm();
    this.keyError.set(null);
  }

  create(): void {
    const name = this.newName().trim();
    if (!name) {
      this.keyError.set('Name is required.');
      return;
    }
    if (this.newPermissions() === 0) {
      this.keyError.set('Select at least one permission.');
      return;
    }
    this.keyError.set(null);
    this.creating.set(true);
    this.api.createApiKey(
      name,
      this.newDescription().trim(),
      this.newPermissions(),
      this.newKeyManual().trim() || undefined,
    ).subscribe({
      next: ck => {
        this.createdKey.set(ck);
        this.apiKeys.update(arr => [
          ...arr,
          {
            id: ck.id,
            name: ck.name,
            description: ck.description,
            permissions: ck.permissions,
            keyPreview: ck.key.slice(0, 12) + '…',
            createdBy: ck.createdBy,
            createdAt: ck.createdAt,
          },
        ]);
        this.creating.set(false);
        this.resetForm();
      },
      error: () => { this.keyError.set('Failed to create API key.'); this.creating.set(false); },
    });
  }

  acknowledgeCreatedKey(): void {
    this.createdKey.set(null);
    this.modalOpen.set(false);
  }

  copyCreatedKey(): void {
    const ck = this.createdKey();
    if (!ck) return;
    navigator.clipboard?.writeText(ck.key).then(() => {
      this.copied.set(true);
      setTimeout(() => this.copied.set(false), 1500);
    });
  }

  removeApiKey(id: string): void {
    this.api.deleteApiKey(id).subscribe({
      next: () => this.apiKeys.update(arr => arr.filter(k => k.id !== id)),
    });
  }

  /** Human label for a key's permission bit-set (table column). */
  permLabel(perms: number): string {
    if ((perms & (ApiKeyPermission.Logs | TRACES_METRICS)) === (ApiKeyPermission.Logs | TRACES_METRICS)) return 'All';
    const parts: string[] = [];
    if (perms & ApiKeyPermission.Logs) parts.push('Logs');
    if (perms & TRACES_METRICS) parts.push('Traces & Metrics');
    return parts.length ? parts.join(', ') : 'None';
  }

  private resetForm(): void {
    this.newName.set('');
    this.newDescription.set('');
    this.newKeyManual.set('');
    this.permLogs.set(true);
    this.permTracesMetrics.set(true);
  }
}
