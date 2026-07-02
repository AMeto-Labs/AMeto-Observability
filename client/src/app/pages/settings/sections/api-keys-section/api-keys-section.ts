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
import { ApiKeyDto, CreatedApiKeyDto } from '../../../../core/models/auth.model';
import { LEVELS } from '../../../../core/models/event.model';
import { SettingsDirtyService } from '../../settings-dirty.service';

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
  readonly newMinimumLevel = signal(0); // index into LEVELS
  readonly newKeyManual = signal('');

  readonly levels = LEVELS;

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
    this.keyError.set(null);
    this.creating.set(true);
    this.api.createApiKey(
      name,
      this.newDescription().trim(),
      this.newMinimumLevel(),
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
            minimumLevel: ck.minimumLevel,
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

  levelLabel(index: number): string {
    return LEVELS[index] ?? 'Verbose';
  }

  private resetForm(): void {
    this.newName.set('');
    this.newDescription.set('');
    this.newMinimumLevel.set(0);
    this.newKeyManual.set('');
  }
}
