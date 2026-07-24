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

const INGEST_ALL = ApiKeyPermission.Logs | ApiKeyPermission.Traces | ApiKeyPermission.Metrics;
const READ_ALL   = ApiKeyPermission.ReadLogs | ApiKeyPermission.ReadTraces | ApiKeyPermission.ReadMetrics;

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

  /** Modal + form state. `editing` holds the key being edited (null = create). */
  readonly modalOpen = signal(false);
  readonly saving = signal(false);
  readonly editing = signal<ApiKeyDto | null>(null);
  readonly newName = signal('');
  readonly newDescription = signal('');
  readonly newKeyManual = signal('');

  /** Per-signal ingest (write) + read scopes. */
  readonly ingestLogs = signal(true);
  readonly ingestTraces = signal(true);
  readonly ingestMetrics = signal(true);
  readonly readLogs = signal(false);
  readonly readTraces = signal(false);
  readonly readMetrics = signal(false);

  /** Combined permission bit-set sent to the server; ≥ 1 scope required. */
  readonly newPermissions = computed(() =>
    (this.ingestLogs()    ? ApiKeyPermission.Logs        : 0) |
    (this.ingestTraces()  ? ApiKeyPermission.Traces      : 0) |
    (this.ingestMetrics() ? ApiKeyPermission.Metrics     : 0) |
    (this.readLogs()      ? ApiKeyPermission.ReadLogs    : 0) |
    (this.readTraces()    ? ApiKeyPermission.ReadTraces  : 0) |
    (this.readMetrics()   ? ApiKeyPermission.ReadMetrics : 0));

  /** Dirty while the create form has input (edit mode is never a persisted-draft dirty). */
  readonly dirty = computed(() => !this.editing() && (
    this.newName().trim().length > 0 || this.newDescription().trim().length > 0 || this.newKeyManual().trim().length > 0));

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
    this.editing.set(null);
    this.resetForm();
    this.createdKey.set(null);
    this.keyError.set(null);
    this.modalOpen.set(true);
  }

  openEditModal(k: ApiKeyDto): void {
    this.editing.set(k);
    this.newName.set(k.name);
    this.newDescription.set(k.description);
    this.newKeyManual.set('');
    this.ingestLogs.set(!!(k.permissions & ApiKeyPermission.Logs));
    this.ingestTraces.set(!!(k.permissions & ApiKeyPermission.Traces));
    this.ingestMetrics.set(!!(k.permissions & ApiKeyPermission.Metrics));
    this.readLogs.set(!!(k.permissions & ApiKeyPermission.ReadLogs));
    this.readTraces.set(!!(k.permissions & ApiKeyPermission.ReadTraces));
    this.readMetrics.set(!!(k.permissions & ApiKeyPermission.ReadMetrics));
    this.createdKey.set(null);
    this.keyError.set(null);
    this.modalOpen.set(true);
  }

  closeCreateModal(): void {
    // Block closing while a key was just created (must be acknowledged).
    if (this.createdKey()) return;
    this.modalOpen.set(false);
    this.editing.set(null);
    this.resetForm();
    this.keyError.set(null);
  }

  save(): void {
    const name = this.newName().trim();
    if (!name) { this.keyError.set('Name is required.'); return; }
    if (this.newPermissions() === 0) { this.keyError.set('Select at least one permission.'); return; }
    this.keyError.set(null);
    this.saving.set(true);

    const edit = this.editing();
    if (edit) {
      this.api.updateApiKey(edit.id, {
        name, description: this.newDescription().trim(), permissions: this.newPermissions(),
      }).subscribe({
        next: () => {
          this.apiKeys.update(arr => arr.map(k => k.id === edit.id
            ? { ...k, name, description: this.newDescription().trim(), permissions: this.newPermissions() } : k));
          this.saving.set(false);
          this.modalOpen.set(false);
          this.editing.set(null);
          this.resetForm();
        },
        error: () => { this.keyError.set('Failed to update API key.'); this.saving.set(false); },
      });
      return;
    }

    this.api.createApiKey(name, this.newDescription().trim(), this.newPermissions(), this.newKeyManual().trim() || undefined)
      .subscribe({
        next: ck => {
          this.createdKey.set(ck);
          this.apiKeys.update(arr => [...arr, {
            id: ck.id, name: ck.name, description: ck.description, permissions: ck.permissions,
            keyPreview: ck.key.slice(0, 12) + '…', createdBy: ck.createdBy, createdAt: ck.createdAt,
          }]);
          this.saving.set(false);
          this.resetForm();
        },
        error: () => { this.keyError.set('Failed to create API key.'); this.saving.set(false); },
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
    const ing: string[] = [];
    if (perms & ApiKeyPermission.Logs)    ing.push('Logs');
    if (perms & ApiKeyPermission.Traces)  ing.push('Traces');
    if (perms & ApiKeyPermission.Metrics) ing.push('Metrics');
    const rd: string[] = [];
    if (perms & ApiKeyPermission.ReadLogs)    rd.push('Logs');
    if (perms & ApiKeyPermission.ReadTraces)  rd.push('Traces');
    if (perms & ApiKeyPermission.ReadMetrics) rd.push('Metrics');

    const parts: string[] = [];
    if (ing.length) parts.push('Ingest ' + ((perms & INGEST_ALL) === INGEST_ALL ? 'all' : ing.join('/')));
    if (rd.length)  parts.push('Read ' + ((perms & READ_ALL) === READ_ALL ? 'all' : rd.join('/')));
    return parts.length ? parts.join(' · ') : 'None';
  }

  private resetForm(): void {
    this.newName.set('');
    this.newDescription.set('');
    this.newKeyManual.set('');
    this.ingestLogs.set(true);
    this.ingestTraces.set(true);
    this.ingestMetrics.set(true);
    this.readLogs.set(false);
    this.readTraces.set(false);
    this.readMetrics.set(false);
  }
}
