import {
  ChangeDetectionStrategy, Component, DestroyRef, ElementRef, computed, effect, inject, signal, untracked, viewChild,
} from '@angular/core';
import { HttpErrorResponse } from '@angular/common/http';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';
import { Subscription } from 'rxjs';

import { ModalComponent, SectionComponent } from '../../../../../shared/components/ui';
import { McpApiService, mcpError } from '../../../../../core/services/mcp-api.service';
import { McpFieldError, McpMaskAction, McpMaskingMode, McpPreview, McpRule } from '../../../../../core/models/mcp.model';
import { McpSectionStore } from '../mcp.store';
import { Segment, cellText, dayOf, fmtCount, hiddenSpan, maskedSentence, sumOf } from '../mcp-format';

interface RuleDraft {
  id: string | null;
  field: string;
  services: string[];
  action: McpMaskAction;
}

/** One line of an event in the preview: a name, and its value in runs of plain and hidden text. */
interface PreviewLine {
  /** The name as stored — `@mt`, or a property's: the same on both sides, so lines are told apart by it. */
  key: string;
  /** The name as the side shows it, in runs: on the sent side, the name an answer carries. */
  name: Segment[];
  parts: Segment[];
  /** True for a property whose name itself held a value, and leaves as its token. */
  renamed?: boolean;
  /** True for a property an answer leaves out altogether. */
  removed?: boolean;
}

/** What a hidden name's mark says, on either side. */
const RENAMED = 'The name itself holds a value: the AI gets its token in place of it.';

/**
 * What a preview as a profile says when the server finds no such profile. That is the one
 * thing it is not found for: the profile, deleted somewhere else after this page read the list.
 */
const goneProfile = (name: string | undefined): string =>
  `${name ? `The profile “${name}”` : 'The profile'} is gone: it was deleted after this page read the list.`;

const ACTIONS: readonly { id: McpMaskAction; label: string; tone: string; does: string }[] = [
  { id: 'hash',      label: 'Hash',        tone: 'acc', does: 'Replace the value with its token. The AI can still match and search by it.' },
  { id: 'keepLast4', label: 'Keep last 4', tone: '',    does: 'Show the last four characters and hide the rest.' },
  { id: 'remove',    label: 'Remove',      tone: 'err', does: 'Leave the property out of the answer.' },
  // A keep vouches for the value at its own path only (docs/MCP.md, Field rules): what is under
  // that path is let through Strict mode, and the detectors go on looking through it.
  { id: 'keep',      label: 'Keep',        tone: 'ok',  does: 'Send the value as stored; in Strict mode this is the allow-list. Properties under it are let through as well, and the detectors still check them.' },
];

/** What a detector checks beyond the shape of the text, where it checks anything. */
const DETECTOR_CHECKS: Readonly<Record<string, string>> = {
  cardNumber: 'Luhn check',
  iin: '12 digits with a check digit',
  iban: 'mod 97 check',
};

/**
 * Masking: which fields never leave as stored, which values are caught wherever they turn up,
 * and — on the right — one real event next to what an answer would carry of it.
 */
@Component({
  selector: 'app-mcp-masking-tab',
  imports: [FormsModule, LucideAngularModule, SectionComponent, ModalComponent],
  templateUrl: './masking-tab.html',
  styleUrl: './masking-tab.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class MaskingTabComponent {
  readonly store = inject(McpSectionStore);
  private readonly api = inject(McpApiService);

  readonly actions = ACTIONS;
  readonly renamedNote = RENAMED;

  /** The mode the tab explains and the preview is taken under. */
  readonly mode = signal<McpMaskingMode>('standard');
  /** The profile the preview is taken as; empty for "no profile, just the mode". */
  readonly previewProfile = signal('');

  readonly modeNote = computed(() => this.mode() === 'strict'
    ? 'Hash every field except those a rule keeps; the other rules below still apply.'
    : 'Hash the fields below and anything a detector finds.');

  // ── Field rules ─────────────────────────────────────────────────────────────

  readonly rows = computed(() => {
    const seen = this.store.summary()?.rules ?? {};
    return this.store.rules().map(rule => {
      const action = ACTIONS.find(a => a.id === rule.action) ?? ACTIONS[0];
      return {
        rule,
        services: rule.services.length ? rule.services.join(', ') : 'All services',
        action: action.label,
        tone: action.tone,
        seen: this.store.summary() ? fmtCount(seen[rule.id] ?? 0) : '—',
      };
    });
  });

  readonly draft = signal<RuleDraft | null>(null);
  readonly draftService = signal('');
  readonly draftError = signal<string | null>(null);
  /** The field of the dialog the error is about, where it is about one. */
  readonly draftErrorField = signal<string | null>(null);
  readonly savingRule = signal(false);
  readonly draftAction = computed(() => ACTIONS.find(a => a.id === this.draft()?.action) ?? ACTIONS[0]);
  readonly serviceOptions = computed(() => this.store.services().filter(s => !(this.draft()?.services ?? []).includes(s)));

  /** True between "Delete rule" and the answer to "really?": the dialog's second step. */
  readonly confirmingDelete = signal(false);
  private readonly keepRule = viewChild<ElementRef<HTMLButtonElement>>('keepRule');
  private readonly askDelete = viewChild<ElementRef<HTMLButtonElement>>('askDelete');
  private backToAskDelete = false;

  /**
   * What deleting the rule in the dialog does to an answer, said of the rule as it is saved —
   * not of what the dialog has been edited to. Taking a rule away is the one click here that
   * can put a hidden value into the next answer, so it is spelled out before it happens.
   */
  readonly deleteNote = computed(() => {
    const rule = this.store.rules().find(r => r.id === this.draft()?.id);
    if (!rule) return '';
    return rule.action === 'keep'
      ? `Without it “${rule.field}” is off the allow-list: Strict mode hides it.`
      : `Without it Standard mode sends “${rule.field}” as it is stored, unless another rule or a detector hides it.`;
  });

  addRule(): void {
    this.openDraft({ id: null, field: '', services: [], action: 'hash' });
  }

  editRule(rule: McpRule): void {
    this.openDraft({ id: rule.id, field: rule.field, services: [...rule.services], action: rule.action });
  }

  private openDraft(draft: RuleDraft): void {
    this.draft.set(draft);
    this.draftService.set('');
    this.draftError.set(null);
    this.draftErrorField.set(null);
    this.confirmingDelete.set(false);
  }

  closeDraft(): void {
    if (!this.savingRule()) this.draft.set(null);
  }

  /** Changes the dialog's rule; a refusal about a field that now holds another value goes with it. */
  patchDraft(change: Partial<RuleDraft>): void {
    this.draft.update(d => (d ? { ...d, ...change } : d));
    const about = this.draftErrorField();
    if (about !== null && about in change) {
      this.draftError.set(null);
      this.draftErrorField.set(null);
    }
  }

  addDraftService(): void {
    const d = this.draft();
    const name = this.draftService().trim();
    if (d && name && !d.services.some(s => s.toLowerCase() === name.toLowerCase())) {
      this.patchDraft({ services: [...d.services, name] });
    }
    this.draftService.set('');
  }

  removeDraftService(name: string): void {
    const d = this.draft();
    if (d) this.patchDraft({ services: d.services.filter(s => s !== name) });
  }

  async saveRule(): Promise<void> {
    const d = this.draft();
    if (!d) return;
    if (!d.field.trim()) {
      this.draftError.set('Field is required.');
      this.draftErrorField.set('field');
      return;
    }
    await this.changeRules(() => this.store.saveRule(d.id, { field: d.field.trim(), services: d.services, action: d.action }),
      'The rule could not be saved.');
  }

  /** Back from the question to the dialog as it was. */
  keepRuleAfterAll(): void {
    this.backToAskDelete = true;
    this.confirmingDelete.set(false);
  }

  /** The second step of "Delete rule": the first one only asks. */
  async deleteRule(): Promise<void> {
    const id = this.draft()?.id;
    if (!id) return;
    await this.changeRules(() => this.store.deleteRule(id), 'The rule could not be deleted.');
    // Refused: the dialog is still open, with the reason in it.
    if (this.draft()) this.keepRuleAfterAll();
    else this.confirmingDelete.set(false);
  }

  /** A change to the rules, after which the preview on the right is no longer what an answer would be. */
  private async changeRules(change: () => Promise<void>, fallback: string): Promise<void> {
    this.draftError.set(null);
    this.draftErrorField.set(null);
    this.savingRule.set(true);
    try {
      await change();
      this.draft.set(null);
      this.retakePreview();
    } catch (e) {
      const problem = mcpError(e, fallback);
      this.draftError.set(problem.error);
      this.draftErrorField.set(problem.field ?? null);
    } finally {
      this.savingRule.set(false);
    }
  }

  // ── Detectors ───────────────────────────────────────────────────────────────

  readonly detectors = computed(() => {
    const summary = this.store.summary();
    return (this.store.overview()?.detectors ?? []).map(d => ({
      ...d,
      note: [
        DETECTOR_CHECKS[d.id],
        !d.on ? 'off' : summary ? `${fmtCount(summary.detectors[d.id] ?? 0)} in 24 h` : null,
      ].filter(Boolean).join(' · '),
    }));
  });

  /** The detector whose switch is on its way to the server; every detector's switch waits for it. */
  readonly switchingDetector = signal<string | null>(null);
  readonly detectorError = signal<string | null>(null);

  async toggleDetector(id: string, on: boolean): Promise<void> {
    if (this.switchingDetector() !== null) return;
    this.switchingDetector.set(id);
    this.detectorError.set(null);
    try {
      await this.store.setDetector(id, on);
      this.retakePreview();
    } catch (e) {
      this.detectorError.set(mcpError(e, 'The detector could not be switched.').error);
    } finally {
      this.switchingDetector.set(null);
    }
  }

  // ── The token ───────────────────────────────────────────────────────────────

  readonly secretSince = computed(() => dayOf(this.store.overview()?.secretCreatedAt ?? ''));
  readonly confirmingRotate = signal(false);
  readonly rotating = signal(false);
  readonly rotateError = signal<string | null>(null);

  async rotateSecret(): Promise<void> {
    this.rotating.set(true);
    this.rotateError.set(null);
    try {
      await this.store.rotateSecret();
      this.confirmingRotate.set(false);
      this.retakePreview();
    } catch (e) {
      this.rotateError.set(mcpError(e, 'The secret could not be rotated.').error);
    } finally {
      this.rotating.set(false);
    }
  }

  // ── What the AI sees ────────────────────────────────────────────────────────

  readonly probe = signal('');
  readonly preview = signal<McpPreview | null>(null);
  readonly previewing = signal(false);
  readonly previewError = signal<string | null>(null);
  /** `filter` when the preview was refused for the filter that picks the event. */
  readonly previewErrorField = signal<string | null>(null);
  private previewRequest: Subscription | null = null;

  constructor() {
    inject(DestroyRef).onDestroy(() => this.previewRequest?.unsubscribe());
    // The question about deleting a rule stands where the button that asked it stood, so the
    // focus would fall out of the dialog: it goes to the answer that changes nothing, and back
    // to the button once that answer is given.
    effect(() => this.keepRule()?.nativeElement.focus());
    effect(() => {
      const button = this.askDelete()?.nativeElement;
      if (!button || !this.backToAskDelete) return;
      this.backToAskDelete = false;
      button.focus();
    });
    // A preview taken as a profile is of the profile as it is: its masking can be changed and
    // saved on the Profiles tab while this one stays mounted. The mode follows it, and the
    // preview is taken again; a profile that is gone leaves the preview to the mode it had.
    effect(() => {
      const id = this.previewProfile();
      const profile = id ? this.store.profiles().find(p => p.id === id) ?? null : null;
      untracked(() => {
        if (!id) return;
        if (!profile) {
          this.previewProfile.set('');
        } else if (profile.maskingMode !== this.mode()) {
          this.mode.set(profile.maskingMode);
          this.retakePreview();
        }
      });
    });
    // The panel opens on the most recent event rather than on nothing.
    this.runPreview();
  }

  /** The preview's "as" control: a mode, or a profile (whose mode it then is). */
  readonly previewAs = computed(() => this.previewProfile() || 'mode:' + this.mode());

  /**
   * The filter that picks the event, as typed. A refusal of the filter stays said in the panel
   * until the next preview, but no longer marks a field that holds another filter by now.
   */
  setProbe(text: string): void {
    this.probe.set(text);
    if (this.previewErrorField() === 'filter') this.previewErrorField.set(null);
  }

  setMode(mode: McpMaskingMode): void {
    this.mode.set(mode);
    this.previewProfile.set('');
    this.retakePreview();
  }

  setPreviewAs(value: string): void {
    if (value.startsWith('mode:')) {
      this.mode.set(value === 'mode:strict' ? 'strict' : 'standard');
      this.previewProfile.set('');
    } else {
      const profile = this.store.profiles().find(p => p.id === value);
      if (!profile) return;
      this.previewProfile.set(profile.id);
      this.mode.set(profile.maskingMode);
    }
    this.retakePreview();
  }

  /**
   * The preview again, after something it depends on changed — where there is one to take
   * again: one shown, or one on its way (whose answer would be of how things were).
   */
  private retakePreview(): void {
    if (this.preview() || this.previewing()) this.runPreview();
  }

  /**
   * Takes the preview. A request still on its way is abandoned first: it was asked under a
   * mode, a profile or a filter that may no longer be the one chosen, and answers do not come
   * back in the order they were asked for — a slow one must not land on top of this one's.
   */
  runPreview(): void {
    this.previewRequest?.unsubscribe();
    this.previewing.set(true);
    this.previewError.set(null);
    this.previewErrorField.set(null);
    const profileId = this.previewProfile();
    // Named as it is asked for: by the time the server says there is no such profile, it may
    // have left the list here as well.
    const profileName = this.store.profiles().find(p => p.id === profileId)?.name;
    this.previewRequest = this.api.preview({
      filter: this.probe().trim() || undefined,
      ...(profileId ? { profileId } : { mode: this.mode() }),
    }).subscribe({
      next: preview => {
        this.preview.set(preview);
        this.previewing.set(false);
      },
      error: e => {
        const gone = !!profileId && e instanceof HttpErrorResponse && e.status === 404;
        const problem: McpFieldError = gone ? { error: goneProfile(profileName) } : mcpError(e, 'The preview could not be taken.');
        this.previewError.set(problem.error);
        this.previewErrorField.set(problem.field ?? null);
        this.previewing.set(false);
        // Out of the lists at once, so "Preview as" no longer offers it: the select falls back
        // to the mode the profile had (see the effect in the constructor), and the sentence
        // above still says what became of it.
        if (gone) this.store.profileGone(profileId);
      },
    });
  }

  /** The stored event, line by line, with what an answer hides of it marked. */
  readonly storedLines = computed<PreviewLine[]>(() => this.lines('stored'));
  /** The same event as an answer carries it. */
  readonly sentLines = computed<PreviewLine[]>(() => this.lines('sent'));

  readonly previewSentence = computed(() => {
    const masked = this.preview()?.masked;
    return masked
      ? maskedSentence(masked.total, { rules: sumOf(masked.rules), strict: sumOf(masked.strict), detectors: sumOf(masked.detectors) })
      : '';
  });

  private lines(side: 'stored' | 'sent'): PreviewLine[] {
    const p = this.preview();
    if (!p?.found || !p.stored || !p.sent) return [];
    const stored = p.stored;
    const sent = p.sent;
    const out: PreviewLine[] = [];
    const named = (key: string): Segment[] => [{ text: key, kind: '' }];
    // A text on both sides: the part that differs is the raw text here and its token there.
    const runs = (was: string, now: string): Segment[] => {
      const span = hiddenSpan(was, now);
      if (!span) return [{ text: was, kind: '' }];
      const middle: Segment = side === 'stored' ? { text: span.stored, kind: 'raw' } : { text: span.sent, kind: 'token' };
      const parts: Segment[] = [{ text: span.head, kind: '' }, middle, { text: span.tail, kind: '' }];
      return parts.filter(part => part.text.length > 0);
    };
    const plain = (key: string, text: string | undefined) => {
      if (text) out.push({ key, name: named(key), parts: [{ text, kind: '' }] });
    };

    plain('@t', stored['@t']);
    plain('@l', stored['@l'] ?? 'Information');
    plain('@service', stored['@service']);
    out.push({ key: '@mt', name: named('@mt'), parts: runs(stored['@mt'] ?? '', sent.template) });

    // A value an answer may leave out altogether: marked on the stored side, named as gone on the other.
    const held = (line: Omit<PreviewLine, 'parts'>, was: string, now: string | undefined) => {
      if (now !== undefined) out.push({ ...line, parts: runs(was, now) });
      else if (side === 'stored') out.push({ ...line, parts: [{ text: was, kind: 'raw' }] });
      else out.push({ ...line, parts: [{ text: 'removed', kind: '' }], removed: true });
    };

    // Paired by the name a property is stored under. An answer can carry it under another: a
    // name that is itself a value — an email address as a key — leaves as its token, like any
    // value would. A server that does not say (`stored` absent) sends every name as stored.
    const answered = new Map(sent.properties.map(prop => [prop.stored ?? prop.name, prop]));
    for (const [key, value] of Object.entries(stored.props ?? {})) {
      const prop = answered.get(key);
      const renamed = prop !== undefined && prop.name !== key;
      // Spelled the cell's way where the cell is this very value: see cellText.
      held({ key, name: renamed ? runs(key, prop.name) : named(key), renamed }, cellText(value, prop?.value), prop?.value);
    }

    const x = stored['@x'];
    if (x) {
      plain('@x.type', x.type);
      if (x.message !== undefined) held({ key: '@x.message', name: named('@x.message') }, x.message, sent.exception?.message);
      if (x.stack !== undefined) held({ key: '@x.stack', name: named('@x.stack') }, x.stack, sent.exception?.stack);
    }
    return out;
  }
}
