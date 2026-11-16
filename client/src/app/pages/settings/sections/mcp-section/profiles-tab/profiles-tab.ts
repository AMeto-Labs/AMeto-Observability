import { ChangeDetectionStrategy, Component, computed, effect, inject, input, signal, untracked } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';

import { ModalComponent, SectionComponent } from '../../../../../shared/components/ui';
import { copyToClipboard } from '../../../../../shared/utils/clipboard';
import { mcpError } from '../../../../../core/services/mcp-api.service';
import { McpMaskingMode, McpProfile, McpProfileInput, McpToolsMode } from '../../../../../core/models/mcp.model';
import { McpSectionStore } from '../mcp.store';
import { dayOf, fmtWindow, lastCall, parseWindow } from '../mcp-format';

/** A profile as its card holds it: the window as typed, the grants as tool ids. */
interface ProfileForm {
  name: string;
  description: string;
  toolsMode: McpToolsMode;
  builtIn: string[];
  published: string[];
  services: string[];
  maskingMode: McpMaskingMode;
  freeFilters: boolean;
  maxEvents: number;
  window: string;
  callsPerMinute: number;
}

/** One switch of the card's tool grid. */
interface ToolSwitch {
  key: string;
  name: string;
  builtIn: boolean;
  on: boolean;
  /** Why the switch cannot be used as things stand; null when it can. */
  blocked: string | null;
  draft: boolean;
}

const NEW = 'new';

/**
 * Profiles: a key and what that key may do. The list on the left, the card of the picked
 * profile — or of a new one — on the right.
 */
@Component({
  selector: 'app-mcp-profiles-tab',
  imports: [FormsModule, LucideAngularModule, SectionComponent, ModalComponent],
  templateUrl: './profiles-tab.html',
  styleUrl: './profiles-tab.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ProfilesTabComponent {
  readonly store = inject(McpSectionStore);

  /** Goes up by one each time another tab asks for a blank profile to be opened here. */
  readonly createRequest = input(0);

  /** The profile in the card: an id, `new`, or null while there is none to show. */
  readonly selectedId = signal<string | null>(null);
  readonly isNew = computed(() => this.selectedId() === NEW);
  readonly selected = computed<McpProfile | null>(() =>
    this.store.profiles().find(p => p.id === this.selectedId()) ?? null);

  readonly form = signal<ProfileForm>(this.blank());
  /** The form as it was loaded or last saved: what "unsaved" is measured against. */
  private readonly baseline = signal<string>('');
  readonly dirty = computed(() => this.selectedId() !== null && JSON.stringify(this.form()) !== this.baseline());

  readonly saving = signal(false);
  readonly error = signal<string | null>(null);
  readonly errorField = signal<string | null>(null);
  readonly serviceDraft = signal('');

  /** The key just created or rotated: shown once, until acknowledged. */
  readonly revealedKey = signal<string | null>(null);
  readonly copied = signal(false);
  readonly confirming = signal<'delete' | 'rotate' | null>(null);
  readonly working = signal(false);

  readonly windowMinutes = computed(() => parseWindow(this.form().window));

  // ── The list ────────────────────────────────────────────────────────────────

  readonly rows = computed(() => {
    const overview = this.store.overview();
    const published = this.store.publishedTools();
    return this.store.profiles().map(p => {
      const granted = published.filter(t => t.profiles.includes(p.id)).length;
      const builtIn = p.toolsMode === 'publishedOnly' ? 0 : (overview?.tools ?? [])
        .filter(t => t.enabled && p.tools.includes(t.name) && (p.freeFilters || !t.takesFreeFilter)).length;
      return {
        profile: p,
        note: [p.description, p.services.length ? p.services.join(', ') : 'all services'].filter(Boolean).join(' · '),
        tools: p.toolsMode === 'publishedOnly'
          ? `Published only · ${granted}`
          : `${builtIn} built-in` + (granted ? ` + ${granted} published` : ''),
        masking: p.maskingMode === 'strict' ? 'Strict' : 'Standard',
        last: lastCall(p.lastCallAt),
      };
    });
  });

  // ── The card ────────────────────────────────────────────────────────────────

  readonly created = computed(() => {
    const p = this.selected();
    return p ? `created by ${p.createdBy || 'unknown'} · ${dayOf(p.createdAt)}` : '';
  });

  readonly toolSwitches = computed<ToolSwitch[]>(() => {
    const f = this.form();
    const publishedOnly = f.toolsMode === 'publishedOnly';
    const builtIn: ToolSwitch[] = (this.store.overview()?.tools ?? []).map(t => ({
      key: 'b:' + t.name,
      name: t.name,
      builtIn: true,
      on: f.builtIn.includes(t.name),
      blocked: publishedOnly ? 'Not offered: the profile takes published tools only.'
        : !t.enabled ? 'Switched off for the whole server on the Tools tab.'
        : t.takesFreeFilter && !f.freeFilters ? 'Withdrawn: free-form filters are off for this profile.'
        : null,
      draft: false,
    }));
    const saved: ToolSwitch[] = this.store.tools().map(t => ({
      key: 'p:' + t.id,
      name: t.name,
      builtIn: false,
      on: f.published.includes(t.id),
      blocked: null,
      draft: !t.published,
    }));
    return [...builtIn, ...saved];
  });

  /** Services that can still be added: known ones the profile does not list yet. */
  readonly serviceOptions = computed(() => this.store.services().filter(s => !this.form().services.includes(s)));

  constructor() {
    // Start on the first profile once the list is there, and move off a profile that is gone.
    effect(() => {
      const profiles = this.store.profiles();
      const id = untracked(this.selectedId);
      if (id === NEW) return;
      if (id === null || !profiles.some(p => p.id === id)) untracked(() => this.open(profiles[0]?.id ?? null));
    });
    effect(() => {
      if (this.createRequest() > 0) untracked(() => this.startNew());
    });
    effect(() => this.store.setDirty('profile', this.dirty()));
  }

  private blank(): ProfileForm {
    return {
      name: '', description: '', toolsMode: 'builtInAndPublished',
      builtIn: (this.store.overview()?.tools ?? []).map(t => t.name),
      published: [], services: [], maskingMode: 'standard', freeFilters: true,
      maxEvents: 200, window: '24 h', callsPerMinute: 30,
    };
  }

  private toForm(p: McpProfile): ProfileForm {
    return {
      name: p.name, description: p.description, toolsMode: p.toolsMode,
      builtIn: [...p.tools], published: [...p.publishedTools], services: [...p.services],
      maskingMode: p.maskingMode, freeFilters: p.freeFilters,
      maxEvents: p.maxEvents, window: fmtWindow(p.maxWindowMinutes), callsPerMinute: p.callsPerMinute,
    };
  }

  /** Puts a profile (or a blank one) into the card, with nothing unsaved. */
  private open(id: string | null): void {
    const profile = id === null || id === NEW ? null : this.store.profiles().find(p => p.id === id) ?? null;
    const form = profile ? this.toForm(profile) : this.blank();
    this.selectedId.set(id);
    this.form.set(form);
    this.baseline.set(JSON.stringify(form));
    this.error.set(null);
    this.errorField.set(null);
    this.serviceDraft.set('');
  }

  private mayDiscard(): boolean {
    return !this.dirty() || confirm('This profile has unsaved changes. Discard them?');
  }

  pick(id: string): void {
    if (id === this.selectedId() || !this.mayDiscard()) return;
    this.open(id);
  }

  startNew(): void {
    if (this.isNew() || !this.mayDiscard()) return;
    this.open(NEW);
  }

  cancel(): void {
    if (this.isNew()) this.open(this.store.profiles()[0]?.id ?? null);
    else this.open(this.selectedId());
  }

  patch(change: Partial<ProfileForm>): void {
    this.form.update(f => ({ ...f, ...change }));
  }

  asCount(value: unknown): number {
    const n = Math.floor(Number(value));
    return Number.isFinite(n) && n > 0 ? n : 0;
  }

  toggleTool(t: ToolSwitch): void {
    if (t.blocked) return;
    const id = t.key.slice(2);
    const flip = (list: string[]) => (list.includes(id) ? list.filter(x => x !== id) : [...list, id]);
    this.form.update(f => (t.builtIn ? { ...f, builtIn: flip(f.builtIn) } : { ...f, published: flip(f.published) }));
  }

  addService(): void {
    const name = this.serviceDraft().trim();
    if (name && !this.form().services.some(s => s.toLowerCase() === name.toLowerCase())) {
      this.patch({ services: [...this.form().services, name] });
    }
    this.serviceDraft.set('');
  }

  removeService(name: string): void {
    this.patch({ services: this.form().services.filter(s => s !== name) });
  }

  async save(): Promise<void> {
    const f = this.form();
    const window = this.windowMinutes();
    if (!f.name.trim()) return this.fail('Name is required.', 'name');
    if (window === null) return this.fail('Write the time window with a unit: 30m, 24h, 7d.', 'maxWindowMinutes');
    if (!f.maxEvents || !f.callsPerMinute) return this.fail('The limits are whole numbers above zero.', 'maxEvents');

    const input: McpProfileInput = {
      name: f.name.trim(), description: f.description.trim(), toolsMode: f.toolsMode, tools: f.builtIn,
      services: f.services, maskingMode: f.maskingMode, freeFilters: f.freeFilters,
      maxEvents: f.maxEvents, maxWindowMinutes: window, callsPerMinute: f.callsPerMinute, publishedTools: f.published,
    };
    this.error.set(null);
    this.errorField.set(null);
    this.saving.set(true);
    try {
      if (this.isNew()) {
        const { profile, key } = await this.store.createProfile(input);
        this.open(profile.id);
        this.revealedKey.set(key);
      } else {
        const id = this.selectedId()!;
        await this.store.updateProfile(id, input);
        this.open(id);
      }
    } catch (e) {
      const problem = mcpError(e, 'The profile could not be saved.');
      this.fail(problem.error, problem.field ?? null);
    } finally {
      this.saving.set(false);
    }
  }

  private fail(message: string, field: string | null): void {
    this.error.set(message);
    this.errorField.set(field);
  }

  // ── The key, and deleting ───────────────────────────────────────────────────

  async rotate(): Promise<void> {
    const id = this.selectedId();
    if (!id || id === NEW) return;
    this.working.set(true);
    try {
      this.revealedKey.set(await this.store.rotateKey(id));
      this.confirming.set(null);
    } catch (e) {
      this.error.set(mcpError(e, 'The key could not be rotated.').error);
      this.confirming.set(null);
    } finally {
      this.working.set(false);
    }
  }

  async remove(): Promise<void> {
    const id = this.selectedId();
    if (!id || id === NEW) return;
    this.working.set(true);
    try {
      // The effect above moves the card to the first profile left once this one is gone.
      this.baseline.set(JSON.stringify(this.form()));
      await this.store.deleteProfile(id);
      this.confirming.set(null);
    } catch (e) {
      this.error.set(mcpError(e, 'The profile could not be deleted.').error);
      this.confirming.set(null);
    } finally {
      this.working.set(false);
    }
  }

  async copyKey(): Promise<void> {
    const key = this.revealedKey();
    if (!key) return;
    await copyToClipboard(key);
    this.copied.set(true);
    setTimeout(() => this.copied.set(false), 1500);
  }

  acknowledgeKey(): void {
    this.revealedKey.set(null);
    this.copied.set(false);
  }
}
