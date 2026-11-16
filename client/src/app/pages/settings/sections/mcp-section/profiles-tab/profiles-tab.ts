import { ChangeDetectionStrategy, Component, computed, effect, inject, input, signal, untracked } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';

import { ModalComponent, SectionComponent } from '../../../../../shared/components/ui';
import { copyToClipboard } from '../../../../../shared/utils/clipboard';
import { mcpError } from '../../../../../core/services/mcp-api.service';
import { McpProfile, McpProfileInput, McpPublishedTool } from '../../../../../core/models/mcp.model';
import { McpSectionStore } from '../mcp.store';
import { asCount, dayOf, isCount, lastCall, parseWindow } from '../mcp-format';
import { focusRow, rowKeyTarget } from '../pick-rows';
import { ProfileForm, blankForm, rebaseForm, sameForm, sameMembers, toForm } from './profile-form';

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

/** The id of the card's error line, for the field it is about to point at. */
const ERROR_ID = 'mcp-profile-error';

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

  readonly errorId = ERROR_ID;
  readonly asCount = asCount;

  /** The profile in the card: an id, `new`, or null while there is none to show. */
  readonly selectedId = signal<string | null>(null);
  readonly isNew = computed(() => this.selectedId() === NEW);
  readonly selected = computed<McpProfile | null>(() =>
    this.store.profiles().find(p => p.id === this.selectedId()) ?? null);

  readonly form = signal<ProfileForm>(this.blank());
  /** The profile as the card was filled with it or last saved it: what "unsaved" is measured against. */
  private readonly baseline = signal<ProfileForm>(this.form());
  readonly dirty = computed(() => this.selectedId() !== null && !sameForm(this.form(), this.baseline()));

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

  /** The fields that cannot be saved as they stand, or that the last refusal was about. */
  readonly invalid = computed(() => {
    const f = this.form();
    const named = this.errorField();
    return {
      name: named === 'name',
      maxEvents: !isCount(f.maxEvents) || named === 'maxEvents',
      window: this.windowMinutes() === null || named === 'maxWindowMinutes',
      callsPerMinute: !isCount(f.callsPerMinute) || named === 'callsPerMinute',
    };
  });

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

  /** The row the Tab key stops at: the picked one — or the first, while a new profile is in the card. */
  readonly tabStop = computed(() => this.selected()?.id ?? this.store.profiles()[0]?.id ?? null);

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
    // The card follows the store: this tab stays mounted while another is shown, and a tool's
    // editor over there grants and withdraws the very tools this card has switches for.
    effect(() => {
      const profiles = this.store.profiles();
      const tools = this.store.tools();
      untracked(() => this.follow(profiles, tools));
    });
    effect(() => {
      if (this.createRequest() > 0) untracked(() => this.startNew());
    });
    effect(() => this.store.setDirty('profile', this.dirty()));
  }

  private blank(): ProfileForm {
    return blankForm((this.store.overview()?.tools ?? []).map(t => t.name));
  }

  /** A form without the grants of tools that are gone: the server refuses an id that is no tool's. */
  private known(form: ProfileForm, tools: readonly McpPublishedTool[] = this.store.tools()): ProfileForm {
    const published = form.published.filter(id => tools.some(t => t.id === id));
    return published.length === form.published.length ? form : { ...form, published };
  }

  /**
   * Brings the card up to date with the store: starts on the first profile once the list is
   * there, moves off a profile that is gone, and otherwise lays what the user has changed over
   * the profile as it is now — with nothing unsaved that is the profile itself.
   */
  private follow(profiles: readonly McpProfile[], tools: readonly McpPublishedTool[]): void {
    const id = this.selectedId();
    const profile = id === null || id === NEW ? null : profiles.find(p => p.id === id) ?? null;
    if (id !== NEW && !profile) {
      this.open(profiles[0]?.id ?? null);
      return;
    }
    const base = this.baseline();
    // A new profile has nothing in the store to follow; only a tool deleted meanwhile leaves it.
    const saved = this.known(profile ? toForm(profile) : base, tools);
    const form = this.known(rebaseForm(this.form(), base, saved), tools);
    if (!sameForm(saved, base)) this.baseline.set(saved);
    if (!sameForm(form, this.form())) this.form.set(form);
  }

  /** Puts a profile (or a blank one) into the card, with nothing unsaved. */
  private open(id: string | null): void {
    const profile = id === null || id === NEW ? null : this.store.profiles().find(p => p.id === id) ?? null;
    const form = this.known(profile ? toForm(profile) : this.blank());
    this.selectedId.set(id);
    this.form.set(form);
    this.baseline.set(form);
    this.error.set(null);
    this.errorField.set(null);
    this.serviceDraft.set('');
  }

  private mayDiscard(): boolean {
    return !this.dirty() || confirm('This profile has unsaved changes. Discard them?');
  }

  /** Puts a profile into the card; false when the card keeps what it has (unsaved input, and the answer was no). */
  pick(id: string): boolean {
    if (id === this.selectedId()) return true;
    if (!this.mayDiscard()) return false;
    this.open(id);
    return true;
  }

  onRowKey(event: KeyboardEvent, index: number): void {
    const rows = this.rows();
    const target = rowKeyTarget(event, index, rows.length);
    if (target === null) return;
    event.preventDefault();
    if (this.pick(rows[target].profile.id)) focusRow(event, target - index);
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

  /** `aria-describedby` of a field: the error line while it is about that field, else nothing. */
  describes(field: string): string | null {
    return this.error() !== null && this.errorField() === field ? ERROR_ID : null;
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
    if (!isCount(f.maxEvents)) return this.fail('Events per call is a whole number above zero.', 'maxEvents');
    if (window === null) return this.fail('Write the time window with a unit: 30m, 24h, 7d.', 'maxWindowMinutes');
    if (!isCount(f.callsPerMinute)) return this.fail('Calls per minute is a whole number above zero.', 'callsPerMinute');

    const input: McpProfileInput = {
      name: f.name.trim(), description: f.description.trim(), toolsMode: f.toolsMode, tools: f.builtIn,
      services: f.services, maskingMode: f.maskingMode, freeFilters: f.freeFilters,
      maxEvents: f.maxEvents, maxWindowMinutes: window, callsPerMinute: f.callsPerMinute,
    };
    // The grants go along only when they were changed here. Left out, the server keeps them as
    // they are — so a save that is about the name cannot undo what a tool's editor just did.
    if (!sameMembers(f.published, this.baseline().published)) input.publishedTools = f.published;

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
      this.fail(mcpError(e, 'The key could not be rotated.').error, null);
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
      // Once the profile is gone from the store the card moves to the first one left, whatever
      // it held. Until then — and when the server refuses — what was typed stays unsaved input.
      await this.store.deleteProfile(id);
      this.confirming.set(null);
    } catch (e) {
      this.fail(mcpError(e, 'The profile could not be deleted.').error, null);
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
