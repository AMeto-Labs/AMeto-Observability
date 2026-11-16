import { ChangeDetectionStrategy, Component, DestroyRef, computed, effect, inject, input, signal, untracked } from '@angular/core';
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
import { focusRow, rowKeyTarget } from '../pick-rows';

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
 * How many refusals a record stands for, when it stands for more than its own: a profile over
 * its calls-per-minute limit has only its 1st, 2nd, 4th, 8th… refusal of a minute recorded,
 * each with the count so far. 0 for a record that is one call and no more.
 */
const repeatsOf = (call: McpCall): number => (typeof call.repeats === 'number' && call.repeats > 1 ? call.repeats : 0);

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
  /** The read of the first page, and the read of the page being added under it. */
  private request: Subscription | null = null;
  private moreRequest: Subscription | null = null;
  /** The filters the calls on screen were read under. */
  private listed = '';
  private wasActive = false;
  private opened = false;

  readonly selectedId = signal<string | null>(null);
  readonly selected = computed<McpCall | null>(() =>
    this.calls().find(c => c.id === this.selectedId()) ?? this.calls()[0] ?? null);

  readonly rows = computed(() => this.calls().map(c => {
    const answered = c.status !== 'Refused';
    const repeats = repeatsOf(c);
    return {
      call: c,
      time: clockTime(c.at),
      day: dayOf(c.at),
      events: answered ? fmtCount(c.events) : '—',
      masked: answered ? fmtCount(c.masked) : '—',
      tokens: answered ? fmtCount(c.tokens) : '—',
      ms: fmtCount(c.elapsedMs),
      tone: statusTone(c.status),
      /** `16` for a record that stands for sixteen refusals of its minute; empty for a single call. */
      repeats: repeats ? fmtCount(repeats) : '',
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
    const repeats = repeatsOf(c);
    return {
      call: c,
      title: `Call ${clockTime(c.at)}`,
      tone: statusTone(c.status),
      result,
      repeats: repeats ? `Refused ${fmtCount(repeats)} times this minute so far — only the 1st, 2nd, 4th, 8th… are recorded.` : '',
      arguments: prettyJson(c.arguments),
      filter: tokenSegments(c.filter),
      filterHasToken: hasToken(c.filter),
      masked: maskedLines(c.maskedBy, this.store.detectorNames()),
      took: `${fmtCount(c.elapsedMs)} ms · ${dayOf(c.at)} ${clockTime(c.at)}`,
    };
  });

  constructor() {
    inject(DestroyRef).onDestroy(() => {
      this.request?.unsubscribe();
      this.moreRequest?.unsubscribe();
    });
    // Any filter that changes, and every return to the tab, reads the first page again. Coming
    // back also brings the figures beside the list up to date — they are of the same record,
    // and it has moved on meanwhile. A filter does not: it cuts the list, not the record. Nor
    // does the first visit: the section has only just read them.
    effect(() => {
      this.profileId(); this.tool(); this.status(); this.hours();
      const active = this.active();
      untracked(() => {
        const returning = active && !this.wasActive && this.opened;
        this.wasActive = active;
        if (!active) return;
        this.opened = true;
        this.load();
        if (returning) this.store.refreshFigures();
      });
    });
  }

  /** The filters as one text: two lists read under the same text are the same cut of the record. */
  private filters(): string {
    return JSON.stringify([this.profileId(), this.tool(), this.status(), this.hours()]);
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

  /** Reads the first page again. */
  load(): void {
    this.request?.unsubscribe();
    // A page on its way was asked for under the list this read replaces: it must not be added
    // under the new one, whenever it arrives.
    this.moreRequest?.unsubscribe();
    this.moreRequest = null;
    this.loadingMore.set(false);
    this.loading.set(true);
    this.error.set(null);
    const filters = this.filters();
    this.request = this.api.getActivity(this.query()).subscribe({
      next: page => {
        this.listed = filters;
        this.calls.set(page.calls);
        this.more.set(page.more);
        this.loading.set(false);
        if (!page.calls.some(c => c.id === this.selectedId())) this.selectedId.set(page.calls[0]?.id ?? null);
      },
      error: e => {
        // Calls read under other filters are not left on screen as if they answered these.
        if (this.listed !== filters) {
          this.calls.set([]);
          this.more.set(false);
        }
        this.error.set(mcpError(e, 'The record of calls could not be read.').error);
        this.loading.set(false);
      },
    });
  }

  /** "Read again": the list as it is filtered, and the figures of the record beside it. */
  reload(): void {
    this.load();
    this.store.refreshFigures();
  }

  loadMore(): void {
    const last = this.calls().at(-1);
    // Not while the first page is being read again: the cursor would be the old list's.
    if (!last || this.loading() || this.loadingMore()) return;
    this.loadingMore.set(true);
    this.error.set(null);
    this.moreRequest = this.api.getActivity({ ...this.query(), afterTs: last.afterTs, afterId: last.id }).subscribe({
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

  onRowKey(event: KeyboardEvent, index: number): void {
    const calls = this.calls();
    const target = rowKeyTarget(event, index, calls.length);
    if (target === null) return;
    event.preventDefault();
    this.pick(calls[target].id);
    focusRow(event, target - index);
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
