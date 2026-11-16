import { ChangeDetectionStrategy, Component, computed, effect, inject, linkedSignal, output, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';

import { SectionComponent } from '../../../../../shared/components/ui';
import { appPath } from '../../../../../shared/utils/app-url';
import { copyToClipboard } from '../../../../../shared/utils/clipboard';
import { mcpError } from '../../../../../core/services/mcp-api.service';
import { McpLimits, McpSummary } from '../../../../../core/models/mcp.model';
import { McpSectionStore } from '../mcp.store';
import { KEY_VARIABLE, McpClientKind, clientSnippets, fmtCount, fmtWindow, parseWindow } from '../mcp-format';

/** The limits as the form holds them: the window as the text a person types. */
interface LimitsForm {
  responseBudgetTokens: number;
  window: string;
  maxEvents: number;
  callsPerMinute: number;
  maxConcurrent: number;
}

interface Tile {
  label: string;
  value: string;
  note: string;
}

const toForm = (limits: McpLimits | undefined): LimitsForm => ({
  responseBudgetTokens: limits?.responseBudgetTokens ?? 0,
  window: limits ? fmtWindow(limits.maxWindowMinutes) : '',
  maxEvents: limits?.maxEvents ?? 0,
  callsPerMinute: limits?.callsPerMinute ?? 0,
  maxConcurrent: limits?.maxConcurrent ?? 0,
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
    return [
      { label: 'Calls', value: fmtCount(s.calls), note: `from ${fmtCount(s.profiles)} ${s.profiles === 1 ? 'profile' : 'profiles'}` },
      {
        label: 'Events returned', value: fmtCount(s.events),
        note: s.medianEvents === null ? 'no call returned any' : `median ${fmtCount(s.medianEvents)} per call`,
      },
      {
        label: 'Values masked', value: fmtCount(s.masked),
        note: s.masked === 0 ? 'nothing was hidden' : `${Math.round((s.maskedBy.rules / s.masked) * 100)} % by field rules`,
      },
      { label: 'Refused', value: fmtCount(s.statuses['Refused'] ?? 0), note: this.refusedNote(s) },
    ];
  });

  private refusedNote(s: McpSummary): string {
    const kinds = Object.entries(s.refusals).filter(([, n]) => n > 0).sort((a, b) => b[1] - a[1]);
    if (kinds.length === 0) return 'none in this window';
    return kinds.slice(0, 2).map(([kind, n]) => `${fmtCount(n)} ${REFUSED_BY[kind] ?? kind}`).join(', ');
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

  readonly limitsDirty = computed(() => {
    const saved = toForm(this.store.overview()?.limits);
    const f = this.form();
    return f.responseBudgetTokens !== saved.responseBudgetTokens || f.maxEvents !== saved.maxEvents
        || f.callsPerMinute !== saved.callsPerMinute || f.maxConcurrent !== saved.maxConcurrent
        || this.windowMinutes() !== (this.store.overview()?.limits.maxWindowMinutes ?? null);
  });

  readonly savingLimits = signal(false);
  readonly limitsError = signal<string | null>(null);

  constructor() {
    effect(() => this.store.setDirty('limits', this.limitsDirty()));
  }

  setLimit<K extends keyof LimitsForm>(key: K, value: LimitsForm[K]): void {
    this.form.update(f => ({ ...f, [key]: value }));
  }

  /** A number field's text as a whole number; nonsense becomes 0, which the save refuses. */
  asCount(value: unknown): number {
    const n = Math.floor(Number(value));
    return Number.isFinite(n) && n > 0 ? n : 0;
  }

  resetLimits(): void {
    this.form.set(toForm(this.store.overview()?.limits));
    this.limitsError.set(null);
  }

  async saveLimits(): Promise<void> {
    const f = this.form();
    const window = this.windowMinutes();
    if (window === null) {
      this.limitsError.set('Write the widest window with a unit: 30m, 24h, 7d.');
      return;
    }
    if (!f.responseBudgetTokens || !f.maxEvents || !f.callsPerMinute || !f.maxConcurrent) {
      this.limitsError.set('Every limit is a whole number above zero.');
      return;
    }
    this.limitsError.set(null);
    this.savingLimits.set(true);
    try {
      // The server holds each limit to its own floor and ceiling and answers with what it kept.
      await this.store.updateSettings({
        responseBudgetTokens: f.responseBudgetTokens, maxWindowMinutes: window, maxEvents: f.maxEvents,
        callsPerMinute: f.callsPerMinute, maxConcurrent: f.maxConcurrent,
      });
      this.form.set(toForm(this.store.overview()?.limits));
    } catch (e) {
      this.limitsError.set(mcpError(e, 'The limits could not be saved.').error);
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
