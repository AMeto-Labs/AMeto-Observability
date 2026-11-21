import { ChangeDetectionStrategy, Component, computed, effect, inject, linkedSignal, output, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';

import { SectionComponent } from '../../../../../shared/components/ui';
import { appPath } from '../../../../../shared/utils/app-url';
import { copyToClipboard } from '../../../../../shared/utils/clipboard';
import { mcpError } from '../../../../../core/services/mcp-api.service';
import { McpLimits, McpSummary } from '../../../../../core/models/mcp.model';
import { McpSectionStore } from '../mcp.store';
import { KEY_VARIABLE, McpClientKind, asCount, clientSnippets, fmtCount, fmtWindow, isCount, parseWindow } from '../mcp-format';

/**
 * The limits as the form holds them: the window as the text a person types, a count as its
 * number field holds it — null while the field is empty.
 */
interface LimitsForm {
  responseBudgetTokens: number | null;
  window: string;
  maxEvents: number | null;
  callsPerMinute: number | null;
  maxConcurrent: number | null;
}

interface Tile {
  label: string;
  value: string;
  note: string;
  /** Why the value is a floor rather than a count, where it is one. */
  title?: string;
}

/**
 * What a figure "at least" stands on: past a profile's calls-per-minute limit only the 1st,
 * 2nd, 4th, 8th… refusal of a minute is recorded, so the calls in between are known only by
 * the count the next record carries — and those after a minute's last record not at all.
 */
const AT_LEAST = 'At least: past a profile’s calls-per-minute limit only the 1st, 2nd, 4th, 8th… refusal of a minute is recorded.';

/** Before a figure that is a floor; with a no-break space, as fmtCount sets its own. */
const AT_LEAST_SIGN = '≥ ';

const toForm = (limits: McpLimits | undefined): LimitsForm => ({
  responseBudgetTokens: limits?.responseBudgetTokens ?? null,
  window: limits ? fmtWindow(limits.maxWindowMinutes) : '',
  maxEvents: limits?.maxEvents ?? null,
  callsPerMinute: limits?.callsPerMinute ?? null,
  maxConcurrent: limits?.maxConcurrent ?? null,
});

const sameLimits = (a: McpLimits | undefined, b: McpLimits | undefined): boolean =>
  !!a && !!b && a.responseBudgetTokens === b.responseBudgetTokens && a.maxWindowMinutes === b.maxWindowMinutes
  && a.maxEvents === b.maxEvents && a.callsPerMinute === b.callsPerMinute && a.maxConcurrent === b.maxConcurrent;

/** How a refusal's kind reads after a count: "5 by limits". */
const REFUSED_BY: Readonly<Record<string, string>> = {
  limit: 'by limits',
  masking: 'by masking',
  input: 'by arguments',
  scope: 'by scope',
  unavailable: 'store unavailable',
};

/**
 * Connection: where the endpoint is and whether it answers, how a client is pointed at it,
 * what the last day looked like, and the ceilings every profile lives under.
 */
@Component({
  selector: 'app-mcp-connection-tab',
  imports: [FormsModule, LucideAngularModule, SectionComponent],
  templateUrl: './connection-tab.html',
  styleUrl: './connection-tab.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ConnectionTabComponent {
  readonly store = inject(McpSectionStore);

  /** "Create a profile key": the section opens the Profiles tab on a blank profile. */
  readonly createProfile = output<void>();

  readonly keyVariable = KEY_VARIABLE;
  readonly clients: readonly { id: McpClientKind; label: string }[] = [
    { id: 'claude-code', label: 'Claude Code' },
    { id: 'cursor',      label: 'Cursor' },
    { id: 'vscode',      label: 'VS Code' },
    { id: 'other',       label: 'Other' },
  ];
  readonly client = signal<McpClientKind>('claude-code');

  readonly enabled = computed(() => this.store.overview()?.enabled ?? false);

  /** How many search slots AI calls may hold at once, as saved — "How access works" quotes it. */
  readonly savedConcurrent = computed(() => this.store.overview()?.limits.maxConcurrent ?? 2);

  /** The endpoint's full address: this page's origin, the deployment prefix, the server's path. */
  readonly url = computed(() => location.origin + appPath(this.store.overview()?.path ?? '/mcp'));

  readonly snippets = computed(() => clientSnippets(this.client(), this.url()));

  readonly switching = signal(false);
  readonly switchError = signal<string | null>(null);

  /** The key of the block copied last, for a moment: its button shows a tick. */
  readonly copied = signal<string | null>(null);

  // ── Last 24 hours ───────────────────────────────────────────────────────────

  readonly tiles = computed<Tile[]>(() => {
    const s = this.store.summary();
    if (!s) {
      return ['Calls', 'Events returned', 'Values masked', 'Refused'].map(label => ({ label, value: '—', note: ' ' }));
    }
    // The record counts the calls it holds. Refusals past a limit that were not recorded one by
    // one are counted beside it — and make every figure they are part of a floor. So does a
    // refusal past a limit at all (`thinned`): the last one of its minute may be past the last
    // record, and then nothing counts it.
    const unrecorded = s.unrecorded ?? 0;
    const isFloor = unrecorded > 0 || s.thinned === true;
    const floor = isFloor ? { title: AT_LEAST } : {};
    const atLeast = (n: number): string => (isFloor ? AT_LEAST_SIGN : '') + fmtCount(n);
    return [
      {
        label: 'Calls', value: atLeast(s.calls + unrecorded), ...floor,
        note: `from ${fmtCount(s.profiles)} ${s.profiles === 1 ? 'profile' : 'profiles'}`,
      },
      {
        label: 'Events returned', value: fmtCount(s.events),
        note: s.medianEvents === null ? 'no call returned any' : `median ${fmtCount(s.medianEvents)} per call`,
      },
      {
        label: 'Values masked', value: fmtCount(s.masked),
        note: s.masked === 0 ? 'nothing was hidden' : `${Math.round((s.maskedBy.rules / s.masked) * 100)} % by field rules`,
      },
      {
        label: 'Refused', value: atLeast((s.statuses['Refused'] ?? 0) + unrecorded), ...floor,
        note: this.refusedNote(s, isFloor),
      },
    ];
  });

  /** The two kinds of refusal there were most of; `limitFloor` when the count of `limit` is a floor. */
  private refusedNote(s: McpSummary, limitFloor: boolean): string {
    const kinds = Object.entries(s.refusals).filter(([, n]) => n > 0).sort((a, b) => b[1] - a[1]);
    if (kinds.length === 0) return 'none in this window';
    return kinds.slice(0, 2)
      .map(([kind, n]) => `${kind === 'limit' && limitFloor ? AT_LEAST_SIGN : ''}${fmtCount(n)} ${REFUSED_BY[kind] ?? kind}`)
      .join(', ');
  }

  // ── Server limits ───────────────────────────────────────────────────────────

  /**
   * The form follows the saved limits, and is the user's from the first keystroke: it is reset
   * only when the saved limits themselves change — not when some other setting of the section
   * was saved and the same limits came back in a new object.
   */
  readonly form = linkedSignal<McpLimits | undefined, LimitsForm>({
    source: () => this.store.overview()?.limits,
    computation: (limits, previous) => (previous && sameLimits(previous.source, limits) ? previous.value : toForm(limits)),
  });

  readonly windowMinutes = computed(() => parseWindow(this.form().window));
  readonly asCount = asCount;

  /** The limits that cannot be saved as they stand: an empty field, a window without a unit. */
  readonly invalid = computed(() => {
    const f = this.form();
    return {
      responseBudgetTokens: !isCount(f.responseBudgetTokens),
      window: this.windowMinutes() === null,
      maxEvents: !isCount(f.maxEvents),
      callsPerMinute: !isCount(f.callsPerMinute),
      maxConcurrent: !isCount(f.maxConcurrent),
    };
  });

  readonly limitsDirty = computed(() => {
    const saved = toForm(this.store.overview()?.limits);
    const f = this.form();
    return f.responseBudgetTokens !== saved.responseBudgetTokens || f.maxEvents !== saved.maxEvents
        || f.callsPerMinute !== saved.callsPerMinute || f.maxConcurrent !== saved.maxConcurrent
        || this.windowMinutes() !== (this.store.overview()?.limits.maxWindowMinutes ?? null);
  });

  readonly savingLimits = signal(false);
  readonly limitsError = signal<string | null>(null);
  /** What the error line is about, when it is about fields of the form rather than about the save. */
  private readonly limitsErrorAbout = signal<'window' | 'counts' | null>(null);

  constructor() {
    effect(() => this.store.setDirty('limits', this.limitsDirty()));
  }

  setLimit<K extends keyof LimitsForm>(key: K, value: LimitsForm[K]): void {
    this.form.update(f => ({ ...f, [key]: value }));
  }

  /**
   * `aria-describedby` of a limit's field: the error line while the field is one it is about,
   * and only until the field is put right — the window for the sentence about the window, the
   * counts that will not do for the other.
   */
  describes(field: keyof LimitsForm): string | null {
    const about = this.limitsErrorAbout();
    const named = about === 'window' ? field === 'window' : about === 'counts' && field !== 'window';
    return named && this.invalid()[field] ? 'mcp-limits-error' : null;
  }

  private failLimits(message: string | null, about: 'window' | 'counts' | null = null): void {
    this.limitsError.set(message);
    this.limitsErrorAbout.set(about);
  }

  resetLimits(): void {
    this.form.set(toForm(this.store.overview()?.limits));
    this.failLimits(null);
  }

  async saveLimits(): Promise<void> {
    const f = this.form();
    const window = this.windowMinutes();
    if (window === null) return this.failLimits('Write the widest window with a unit: 30m, 24h, 7d.', 'window');
    if (!isCount(f.responseBudgetTokens) || !isCount(f.maxEvents) || !isCount(f.callsPerMinute) || !isCount(f.maxConcurrent)) {
      return this.failLimits('Every limit is a whole number above zero.', 'counts');
    }
    this.failLimits(null);
    this.savingLimits.set(true);
    try {
      // The server holds each limit to its own floor and ceiling and answers with what it kept.
      await this.store.updateSettings({
        responseBudgetTokens: f.responseBudgetTokens, maxWindowMinutes: window, maxEvents: f.maxEvents,
        callsPerMinute: f.callsPerMinute, maxConcurrent: f.maxConcurrent,
      });
      this.form.set(toForm(this.store.overview()?.limits));
    } catch (e) {
      this.failLimits(mcpError(e, 'The limits could not be saved.').error);
    } finally {
      this.savingLimits.set(false);
    }
  }

  // ── The switch, and copying ─────────────────────────────────────────────────

  async toggleEnabled(): Promise<void> {
    this.switching.set(true);
    this.switchError.set(null);
    try {
      await this.store.updateSettings({ enabled: !this.enabled() });
    } catch (e) {
      this.switchError.set(mcpError(e, 'The endpoint could not be switched.').error);
    } finally {
      this.switching.set(false);
    }
  }

  async copy(key: string, text: string): Promise<void> {
    await copyToClipboard(text);
    this.copied.set(key);
    setTimeout(() => { if (this.copied() === key) this.copied.set(null); }, 1500);
  }
}
