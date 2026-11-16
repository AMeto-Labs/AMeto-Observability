import { ChangeDetectionStrategy, Component, computed, effect, inject, input, signal, untracked } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { LucideAngularModule } from 'lucide-angular';
import { Subscription } from 'rxjs';

import { SectionComponent } from '../../../../../shared/components/ui';
import { McpApiService, mcpError } from '../../../../../core/services/mcp-api.service';
import { McpActivityQuery, McpCall } from '../../../../../core/models/mcp.model';
import { McpSectionStore } from '../mcp.store';
import {
  REFUSAL_KINDS, clockTime, dayOf, fmtCount, hasToken, maskedLines, prettyJson, statusTone, tokenSegments,
} from '../mcp-format';

/** The service the calls are recorded under; the Logs page finds them by it. */
const AUDIT_SERVICE = 'ameto-mcp';

/** The windows the list offers, each with the Logs page's own range that covers it. */
const RANGES = [
  { hours: 1,   label: 'Last hour',     preset: '1d' },
  { hours: 24,  label: 'Last 24 hours', preset: '1d' },
  { hours: 72,  label: 'Last 3 days',   preset: '3d' },
  { hours: 168, label: 'Last 7 days',   preset: '7d' },
] as const;

const STATUSES = ['OK', 'Trimmed', 'Refused', 'Timed out', 'Failed'] as const;
const PAGE = 50;

const quote = (value: string): string => `'${value.replace(/\\/g, '\\\\').replace(/'/g, '\\\'')}'`;

/**
 * Activity: the record of the assistants' calls, newest first, and one call taken apart — who
 * asked, with which arguments, how much came back and what was hidden from it. The calls are
 * ordinary log events of the service `ameto-mcp`; "Open in Logs" shows the same ones there.
 */
@Component({
  selector: 'app-mcp-activity-tab',
  imports: [FormsModule, LucideAngularModule, SectionComponent],
  templateUrl: './activity-tab.html',
  styleUrl: './activity-tab.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ActivityTabComponent {
  readonly store = inject(McpSectionStore);
  private readonly api = inject(McpApiService);
  private readonly router = inject(Router);

  /** True while this tab is the one shown: coming back to it reads the list again. */
  readonly active = input(false);

  readonly ranges = RANGES;
  readonly statuses = STATUSES;

  readonly profileId = signal('');
  readonly tool = signal('');
  readonly status = signal('');
  readonly hours = signal<number>(24);

  /** Every tool a call can name: the built-in ones, then the saved ones. */
  readonly toolNames = computed(() => [
    ...(this.store.overview()?.tools ?? []).map(t => t.name),
    ...this.store.tools().map(t => t.name),
  ]);

  readonly calls = signal<McpCall[]>([]);
  readonly more = signal(false);
  readonly loading = signal(false);
  readonly loadingMore = signal(false);
  readonly error = signal<string | null>(null);
  private request: Subscription | null = null;
  private readBefore = false;

  readonly selectedId = signal<string | null>(null);
  readonly selected = computed<McpCall | null>(() =>
    this.calls().find(c => c.id === this.selectedId()) ?? this.calls()[0] ?? null);

  readonly rows = computed(() => this.calls().map(c => {
    const answered = c.status !== 'Refused';
    return {
      call: c,
      time: clockTime(c.at),
      day: dayOf(c.at),
      events: answered ? fmtCount(c.events) : '—',
      masked: answered ? fmtCount(c.masked) : '—',
      tokens: answered ? fmtCount(c.tokens) : '—',
      ms: fmtCount(c.elapsedMs),
      tone: statusTone(c.status),
    };
  }));

  /** `Showing 14 of 1 284 calls` where the whole is known: the unfiltered last day. */
  readonly footer = computed(() => {
    const shown = this.calls().length;
    const summary = this.store.summary();
    const unfiltered = !this.profileId() && !this.tool() && !this.status() && this.hours() === 24;
    const total = unfiltered && summary && !summary.partial && summary.calls >= shown ? ` of ${fmtCount(summary.calls)}` : '';
    return `Showing ${fmtCount(shown)}${total} ${shown === 1 && !total ? 'call' : 'calls'}`;
  });

  // ── The picked call ─────────────────────────────────────────────────────────

  readonly detail = computed(() => {
    const c = this.selected();
    if (!c) return null;
    const budget = this.store.overview()?.limits.responseBudgetTokens;
    const size = `about ${fmtCount(c.tokens)}${budget ? ' of ' + fmtCount(budget) : ''} tokens`;
    const result = c.status === 'OK' ? `${fmtCount(c.events)} ${c.events === 1 ? 'event' : 'events'}, ${size}`
      : c.status === 'Trimmed' ? `${fmtCount(c.events)} events, ${size}: cut to the budget, with a cursor to continue`
      : c.status === 'Timed out' ? `Ran out of its time budget after ${fmtCount(c.elapsedMs)} ms; what it had found was returned as a part`
      : c.status === 'Failed' ? 'Failed on the server; the server log has the details'
      : `Refused by ${REFUSAL_KINDS[c.refusalKind ?? ''] ?? 'the server'}`;
    return {
      call: c,
      title: `Call ${clockTime(c.at)}`,
      tone: statusTone(c.status),
      result,
      arguments: prettyJson(c.arguments),
      filter: tokenSegments(c.filter),
      filterHasToken: hasToken(c.filter),
      masked: maskedLines(c.maskedBy, this.store.detectorNames()),
      took: `${fmtCount(c.elapsedMs)} ms · ${dayOf(c.at)} ${clockTime(c.at)}`,
    };
  });

  constructor() {
    // Any filter that changes, and every return to the tab, reads the first page again.
    effect(() => {
      this.profileId(); this.tool(); this.status(); this.hours();
      if (this.active()) untracked(() => this.load());
    });
  }

  private query(): McpActivityQuery {
    return {
      profile: this.profileId() || undefined,
      tool: this.tool() || undefined,
      status: this.status() || undefined,
      from: new Date(Date.now() - this.hours() * 3_600_000).toISOString(),
      count: PAGE,
    };
  }

  load(): void {
    this.request?.unsubscribe();
    this.loading.set(true);
    this.error.set(null);
    this.request = this.api.getActivity(this.query()).subscribe({
      next: page => {
        this.calls.set(page.calls);
        this.more.set(page.more);
        this.loading.set(false);
        if (!page.calls.some(c => c.id === this.selectedId())) this.selectedId.set(page.calls[0]?.id ?? null);
      },
      error: e => {
        this.error.set(mcpError(e, 'The record of calls could not be read.').error);
        this.loading.set(false);
      },
    });
    // The figures beside the list are of the same record, so a list read again brings them up
    // to date as well. The first read needs none: the section has only just asked for them.
    if (this.readBefore) this.store.refreshFigures();
    this.readBefore = true;
  }

  loadMore(): void {
    const last = this.calls().at(-1);
    if (!last || this.loadingMore()) return;
    this.loadingMore.set(true);
    this.api.getActivity({ ...this.query(), afterTs: last.afterTs, afterId: last.id }).subscribe({
      next: page => {
        this.calls.update(calls => [...calls, ...page.calls]);
        this.more.set(page.more);
        this.loadingMore.set(false);
      },
      error: e => {
        this.error.set(mcpError(e, 'More calls could not be read.').error);
        this.loadingMore.set(false);
      },
    });
  }

  pick(id: string): void {
    this.selectedId.set(id);
  }

  // ── The same calls on the Logs page ─────────────────────────────────────────

  private preset(hours: number): string {
    return RANGES.find(r => r.hours >= hours)?.preset ?? '7d';
  }

  /** The list as it is filtered here. */
  openListInLogs(): void {
    const clauses = [`@service = ${quote(AUDIT_SERVICE)}`];
    if (this.profileId()) clauses.push(`ProfileId = ${quote(this.profileId())}`);
    if (this.tool()) clauses.push(`Tool = ${quote(this.tool())}`);
    if (this.status()) clauses.push(`Status = ${quote(this.status())}`);
    void this.router.navigate(['/events'], { queryParams: { filter: clauses.join(' and '), preset: this.preset(this.hours()) } });
  }

  /** One call's profile and tool, over a window that reaches back to it. */
  openCallInLogs(call: McpCall): void {
    const clauses = [`@service = ${quote(AUDIT_SERVICE)}`];
    if (call.profileId) clauses.push(`ProfileId = ${quote(call.profileId)}`);
    if (call.tool) clauses.push(`Tool = ${quote(call.tool)}`);
    const ageHours = (Date.now() - new Date(call.at).getTime()) / 3_600_000;
    void this.router.navigate(['/events'], { queryParams: { filter: clauses.join(' and '), preset: this.preset(ageHours) } });
  }
}
