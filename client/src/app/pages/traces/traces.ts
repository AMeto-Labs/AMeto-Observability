import {
  Component, signal, computed, inject, OnInit, ElementRef,
  OnDestroy, HostListener, ChangeDetectionStrategy, ChangeDetectorRef,
  viewChild, viewChildren, afterRenderEffect,
} from '@angular/core';
import { injectVirtualizer } from '@tanstack/angular-virtual';
import { Observable, Subscription } from 'rxjs';
import { Router, ActivatedRoute } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';
import { format } from 'date-fns';
import { ApiService } from '../../core/services/api.service';
import { serviceColor } from '../../shared/utils/service-color';
import { EventDto } from '../../core/models/event.model';
import { SpanDto, TraceRowDto, TraceStatsDto } from '../../core/models/span.model';
import { StreamEndDto, StreamFrame } from '../../core/models/stream.model';
import { subHours, formatISO } from 'date-fns';
import { ServiceGraphComponent } from './service-graph/service-graph';
import { FlamegraphComponent } from './flame-graph/flame-graph';
import { LatencyComponent } from './latency/latency';
import { CompareTraceComponent } from './compare-trace/compare-trace';
import { SuggestInputDirective } from '../../shared/suggest/suggest-input.directive';
import { ModalComponent } from '../../shared/components/ui';
import { EventDetailComponent } from '../events/components/event-detail/event-detail';
import { EventListRowComponent } from '../events/components/event-list-row/event-list-row';
import { PropertyMenuComponent } from '../../shared/components/property-menu/property-menu';
import { SearchHistoryComponent } from '../../shared/components/search-history/search-history';
import { SearchHistoryService } from '../../core/services/search-history.service';

/**
 * Who asked for a load of the trace list.
 *
 * It answers ONE question — may this stream own the failure? — and it is deliberately not the
 * answer to "may this stream paint as it goes?", which is a question about the rows on screen.
 * The two were once the same expression (`inPlace && traces().length > 0`), and an empty list
 * made them disagree: the 15 s poll took the user's branch, so one flaky tick raised a banner
 * for a search nobody ran, offered Stop on an unattended refresh, kept the background failure
 * counter (and its backoff) at zero, and froze the list for the life of the page.
 *
 * 'background' is the poll's silent refresh and the return to the Traces tab — both re-run the
 * query the list is ALREADY showing. Everything else is 'user': Apply, Run, Refresh, Clear, a
 * range chip, a seek from the property menu, and the page's first load.
 */
export type LoadOrigin = 'user' | 'background';

/**
 * How the last stream that owned the list ended, in the only vocabulary the header speaks.
 *
 * <p>It is the SERVER's account, normalised — not a re-derivation of it. The client used to
 * decide this by counting its own rows against the `max` it had asked for, which can answer
 * exactly one of the cases below and answers it by coincidence: a list of exactly `max` rows is
 * capped, and everything else is silently called complete.</p>
 *
 * <p>THE ENDING IS THE CEILING AXIS ONLY. Whether part of the window went missing on the way is
 * a SEPARATE fact ({@link SegmentLoss}), held in a separate signal, because the server holds it
 * separately too — `reason` and `truncatedBy` are two fields precisely because "your row ceiling
 * stopped me" and "I could not read part of the window" are both true at once, routinely. This
 * page used to fold them into one `capped-lost` value, and folding is what let the frame that
 * happened to carry the loss decide how loud it was: the loss rides `truncatedBy` when the
 * ceiling bit first and rides a `query-error` sentence when the walk reached the end of the
 * window, and the two produced two different screens for one dead file. Unfolded, the loss is
 * rendered by the loss and the ceiling by the ceiling.</p>
 *
 * <p>`null` is the absence of an ending — no stream has finished yet, or the list was emptied —
 * and is deliberately NOT the same value as `read-out`, which is a positive claim by the server
 * that the whole window was read. The header renders both as a bare count; only one of them
 * means it.</p>
 */
export type ListEnding =
  /** `complete: true` — the whole requested window was read out. */
  | { kind: 'read-out' }
  /** `max-rows` — the row ceiling stopped it. */
  | { kind: 'capped' }
  /** The server denied completeness for a reason this client cannot name. */
  | { kind: 'short' };

/**
 * Part of the queried window is not in the answer, and never was — the `truncatedBy` vocabulary.
 *
 * <p>The two differ in ONE thing that matters to the reader, and it is not severity: it is
 * whether asking again differently can help. A segment the search ran out of room to open comes
 * back when the window is narrower; a segment that will not open does not come back at all.</p>
 */
export type SegmentLoss = 'unread-segment' | 'unreadable-segment';

/**
 * WHAT THE SERVER WOULD HAVE SAID, held here for the frame that cannot say it.
 *
 * <p>These are the server's own two truncation sentences, copied verbatim from
 * `TraceQueryEndpointMapper.FinishStreamAsync`. They exist on this side because the server can
 * only spell a loss out in prose on the ending where it has prose to spell: a `query-error`.
 * When the row ceiling bites first the SAME loss rides the terminal `done` frame as a
 * `truncatedBy` code with no sentence attached — and a code the page renders as a grey footnote
 * while the sentence gets a banner is one dead file wearing two faces.</p>
 *
 * <p>So the page writes the sentence itself in that case, and writes the SAME one, so that what
 * the operator sees is decided by which segment vanished and not by how many rows happened to
 * fit above it. The copy is the cost: a re-worded sentence on the server drifts from this one
 * until someone copies it across. That is a wording drift, not a treatment drift — the loud
 * screen is raised by the `truncatedBy` code either way — and the only thing that would remove
 * it is a machine-readable cause on the `query-error` frame, which is server work.</p>
 */
const LOSS_SENTENCE: Readonly<Record<SegmentLoss, string>> = {
  'unreadable-segment':
    'Results are truncated: a storage segment inside this window could not be read — it was '
    + 'deleted or damaged — so the traces it held are missing from this list. Narrowing the time '
    + 'window will not bring them back; the server log names the file.',
  'unread-segment':
    'Results are truncated: part of this window sits inside a storage segment the search ran out '
    + 'of room to open before it had to move on, so the traces it holds are missing from this '
    + 'list. Narrow the time window to bring them back into reach.',
};

/** TraceQL vocabulary offered by the Ctrl+Space autocomplete: intrinsics, common OTel span
 *  attributes (dotted), status/kind enum values, and the comparison/boolean operators. */
const TRACEQL_TOKENS: readonly string[] = [
  // intrinsics
  'status', 'duration', 'name', 'service', 'kind',
  // common span / resource attributes
  '.http.status_code', '.http.request.method', '.http.route', '.http.target', '.http.url',
  '.http.response.status_code', '.rpc.method', '.rpc.service', '.db.system', '.db.statement',
  '.db.name', '.net.peer.name', '.messaging.system', '.error',
  // enum values
  'error', 'ok', 'unset',
  'server', 'client', 'producer', 'consumer', 'internal',
  // operators / duration units
  '&&', '||', '=', '!=', '>', '>=', '<', '<=', 'ms', 's',
];

@Component({
  selector: 'app-traces',
  imports: [FormsModule, LucideAngularModule, ServiceGraphComponent, FlamegraphComponent, LatencyComponent, CompareTraceComponent, SuggestInputDirective, ModalComponent, EventDetailComponent, EventListRowComponent, PropertyMenuComponent, SearchHistoryComponent],
  templateUrl: './traces.html',
  styleUrl: './traces.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class TracesComponent implements OnInit, OnDestroy {
  private api    = inject(ApiService);
  private cdr    = inject(ChangeDetectorRef);
  private router = inject(Router);
  private route  = inject(ActivatedRoute);
  /** This page's slice of the shared per-user search history (scope keeps it apart
   *  from the Events/Metrics lists — see SearchHistoryService.forScope). */
  private history = inject(SearchHistoryService).forScope('traces');

  // ── Loading / data ────────────────────────────────────────────────────────
  statsLoading  = signal(false);
  stats         = signal<TraceStatsDto | null>(null);
  loading       = signal(false);
  traces        = signal<TraceRowDto[]>([]);

  selectedTraceId = signal<string | null>(null);
  traceSpans      = signal<SpanDto[]>([]);
  traceLoading    = signal(false);
  selectedSpan    = signal<SpanDto | null>(null);

  activeMainTab:  'traces' | 'graph' | 'latency' | 'compare' = 'traces';
  activeTraceTab: 'timeline' | 'flamegraph' | 'details' = 'timeline';
  activeSpanTab:  'tags' | 'logs' = 'tags';

  // Trace logs (correlated by @tr — primary trace↔logs view).
  // Loaded once per trace; the selected span narrows them client-side.
  traceLogs        = signal<EventDto[]>([]);
  traceLogsLoading = signal(false);
  traceLogsLoaded  = signal(false);
  onlyThisSpan     = signal(false);

  /** Log opened in the full-detail modal (the same renderer the Events page uses); null = closed. */
  logModalEvent    = signal<EventDto | null>(null);

  /** Logs shown in the Logs tab: all trace logs, or only those of the selected span. */
  visibleLogs = computed(() => {
    const all = this.traceLogs();
    if (!this.onlyThisSpan()) return all;
    const sp = this.selectedSpan()?.spanId;
    if (!sp) return all;
    return all.filter(l => l['@sp'] === sp);
  });

  /** How many logs belong to the currently selected span (for the badge). */
  spanLogCount = computed(() => {
    const sp = this.selectedSpan()?.spanId;
    if (!sp) return 0;
    return this.traceLogs().reduce((n, l) => n + (l['@sp'] === sp ? 1 : 0), 0);
  });

  // ── Filters ───────────────────────────────────────────────────────────────
  traceIdInput       = '';
  filterName         = '';
  filterService      = '';
  filterStatus       = '';
  filterMinDurationMs: number | null = null;
  filterMaxDurationMs: number | null = null;
  filterHttpStatus   = '';
  preset             = '1h';
  /** Custom [from,to] as datetime-local strings — used when preset === 'custom'. */
  customFrom         = '';
  customTo           = '';

  /** Stable query window driving the Graph / Latency panels. Set on user action only, so the
   *  15 s live poll never changes it → those panels don't re-fetch/re-layout (no jumping). */
  readonly winFrom     = signal<string>('');
  readonly winTo       = signal<string | undefined>(undefined);
  /** Full service list from the backend (not just services present in loaded traces). */
  readonly allServices = signal<string[]>([]);

  // TraceQL
  traceqlInput   = '';
  traceqlMode    = signal(false);
  /** The SERVER'S OWN SENTENCE for a search the USER ran that ended in an error — '' when none
   *  did. Despite the name it is not TraceQL-specific: the filter-bar path streams too, and dies
   *  the same ways. A failing BACKGROUND refresh must never set it: see the error branch of
   *  startStream.
   *
   *  It is no longer read directly by anything that RENDERS. Everything downstream — the banner
   *  strip, the header suffix, the hold — goes through {@link listBanner}, because this is only
   *  one of the two ways the same news arrives: a vanished segment reported at the end of the
   *  window lands here as a sentence, and the identical segment reported at the row ceiling
   *  lands in {@link listLoss} as a code with no sentence at all. Anything reading this signal
   *  alone renders one of those and not the other, which is exactly how one dead file came to
   *  look like an error at max=1000 and like a footnote at max=50. */
  traceqlError   = signal('');
  /** Candidates for the TraceQL Ctrl+Space autocomplete. */
  readonly traceqlSuggestions = TRACEQL_TOKENS as string[];

  /** Right-hand search-history panel — mounted under @if so opening reloads. */
  historyOpen = signal(false);

  private _poll: ReturnType<typeof setInterval> | null = null;

  // ── Streaming ─────────────────────────────────────────────────────────────
  // The list arrives over SSE, one row at a time. There is no request epoch here and no
  // page bookkeeping: the epoch counter existed only because a slow response from an
  // ABANDONED search could splice itself into the list of the search that replaced it, and
  // unsubscribing the previous stream before starting the next one is that same guarantee
  // made structural — a cancelled EventSource has no callbacks left to fire. Deleted, not
  // forgotten.

  /** The one stream that may own the list. Non-null exactly while rows can still arrive. */
  private streamSub: Subscription | null = null;
  /** Publishes the current stream's buffer into `traces`, or null while the stream has
   *  nothing it may hand over yet (a silent refresh — see startStream). */
  private flushPending: (() => void) | null = null;
  /** True while ANY stream is open: the poll stands back (never two at once) and the Refresh
   *  icon spins, which is the whole visible footprint of a background refresh. */
  readonly streaming = signal(false);
  /** True while the open stream is one the USER asked for. The "streaming…" hint and the Stop
   *  button belong to this one: a background refresh has nothing the user needs to interrupt,
   *  and offering Stop twice a minute is just flicker — including over an empty list, which is
   *  a fact about the rows and never about who asked. What it is NOT is "this stream reaches
   *  the rows": a background refresh of an empty list does paint into it (streamPaints), and
   *  that is the flag stopStream reads. */
  readonly streamPublishing = signal(false);
  /** Whether the open stream may put rows on screen at all — true for every user-initiated
   *  search and for a background refresh that found the list empty. Read by stopStream: only
   *  a stream that was painting can leave a PARTIAL list behind, and only then may the header
   *  call it partial. Not a signal: nothing renders it, it only qualifies the stamp. */
  private streamPaints = false;
  /** How the last stream to own the list ended — the server's own account of it, normalised by
   *  {@link endingOf}. Null until a stream has finished, and again whenever the list is emptied:
   *  an ending describes rows, and outlives nothing else. */
  readonly listEnding = signal<ListEnding | null>(null);
  /**
   * Part of the window on screen is MISSING, and which kind of missing — null when nothing is.
   *
   * <p>Separate from {@link listEnding} because it has a different lifetime, and that difference
   * is the whole of it. An ending describes ONE READ: the next read replaces it, and a read that
   * runs to the bottom of the window genuinely disproves the last one that did not. A lost
   * segment describes the STORAGE, and no read can disprove it — nothing re-reads a file that is
   * gone. The server says as much where it remembers them (VanishedRegionLog: "the consumer may
   * never decide a later page has made the fault good").</p>
   *
   * <p>Which is why nothing in the streaming path ever CLEARS this. A stream may only set it
   * (see the `complete` handler); it goes back to null in exactly one place — a search the USER
   * ran, emptying the list. Before that rule existed, a background tick was enough to retire the
   * warning: end a user search with `{max-rows, unreadable-segment}`, restart the server so the
   * in-process memory of the dead segment is gone, and the 15 s poll came back
   * `{complete:true, exhausted}` and quietly relabelled a window with a permanent hole in it as
   * whole — the same lie the server-side memory was built to kill, reached by waiting instead of
   * by clicking. An unattended refresh may replace rows and may make the page LESS confident; it
   * may not make it more confident than the user left it.</p>
   */
  readonly listLoss = signal<SegmentLoss | null>(null);
  /** The `max` the request behind the rows on screen actually carried, so the header names the
   *  ceiling that was asked for rather than the one this page would ask for next. Identical
   *  today (every caller sends {@link streamMax}); it stops being identical the first time a
   *  caller passes anything else, and a hint that reads "newest 2000" over a 500-row ask is a
   *  wrong number, not a stale one. */
  readonly listMax = signal(2000);
  /** True when the last stream ended because it hit its row ceiling, so older traces exist
   *  beyond what is on screen and the header must say so rather than imply completeness.
   *  Derived rather than stored, so it cannot disagree with the ending it is a fact about. */
  readonly streamCapped = computed(() => this.listEnding()?.kind === 'capped');
  /**
   * The one sentence that goes above the filter bar — '' when there is none.
   *
   * <p>Two sources, one strip, and deliberately so. The server sends its own sentence when the
   * ending it reached has one (a `query-error`); when the row ceiling bit first, the very same
   * loss arrives as a `truncatedBy` code on a `done` frame with no sentence at all, and the page
   * supplies the one the server would have sent ({@link LOSS_SENTENCE}). Rendering only the
   * first of those is how one vanished segment came to look like an error at max=1000 and like a
   * footnote at max=50.</p>
   *
   * <p>A failure the page could not classify wins over a loss it could: `traceqlError` means a
   * search the user ran did not finish, which is a bigger claim about the rows than a hole in
   * them, and the server's own words are always the better sentence when there are any. In
   * practice the two never coexist — a stream that ends in an error reports no `truncatedBy`,
   * and a user's stream clears both before it starts.</p>
   */
  readonly listBanner = computed<string>(() => {
    const spoke = this.traceqlError();
    if (spoke) return spoke;
    const loss = this.listLoss();
    return loss ? LOSS_SENTENCE[loss] : '';
  });
  /**
   * The list header's account of an ending, with the long version in its tooltip — or null when
   * there is nothing to add to the count (nothing has ended, or the server said it read the
   * whole window out).
   *
   * <p>The two losses keep different ADVICE, which is the whole reason `truncatedBy` is carried
   * this far: a segment the search ran out of room to open comes back when the window is
   * narrower, and a segment that will not open does not come back at all — telling the second
   * user to narrow their window sends them round a loop that cannot help them. That advice now
   * leads with {@link listBanner}, where it is a sentence rather than a tooltip, and the suffix
   * echoes it in its `title`. The suffix itself says only what every road to it can support.</p>
   */
  readonly listEndHint = computed<{ text: string; title: string } | null>(() => {
    const loss   = this.listLoss();
    const spoke  = this.listBanner() !== '';
    const end    = this.listEnding();
    const capped = end?.kind === 'capped';

    // ── THE FRAME CATEGORY DOES NOT APPEAR IN THIS BRANCH, AND THAT IS THE POINT ───────────
    //
    // One vanished segment reaches this page down two different roads. Walk the window to its
    // floor with a hole in it and the server ends the stream with a `query-error` sentence;
    // reach the row ceiling on the way and the SAME hole rides the terminal `done` frame as
    // `truncatedBy`. streamJson routes the first to `subscriber.error` and the second to an
    // ending value — two categories, and the page used to read the category as the severity:
    // red banner and a frozen list at max=1000, a grey count suffix and a live list at max=50,
    // for one and the same dead file, chosen by how many rows happened to fit above it.
    //
    // So the LOSS picks the treatment and the ceiling is only ever a clause on top of it. Both
    // roads now end here: a standing sentence above the filter bar, a list held off the poll,
    // and a suffix that calls the rows partial and points at the sentence. What the two roads
    // still do not share is DETAIL — only one of them knows the ceiling also bit — and detail
    // the page has is not a reason to withhold it. Detail it does NOT have is the other half:
    // a `query-error` arrives as prose, so this page cannot tell a lost segment from a spent
    // deadline there, and refuses to guess by pattern-matching an English sentence. Saying
    // "partial, and the message says why" is true of every ending that reaches this branch.
    if (loss || spoke) {
      if (!this.traces().length)
        // No rows at all — a parse error, or a refusal before the first one. An empty list is
        // not a partial one, so this does not call it that.
        return {
          text:  '· the search failed — see the message above',
          title: 'The message above the filter bar is the server\'s own account of why nothing '
               + 'came back.',
        };
      return {
        text:  capped
          ? `· newest ${this.listMax()}, partial list — see the message above`
          : '· partial list — see the message above',
        title: loss
          ? (loss === 'unreadable-segment'
              ? 'Part of this window sits in a storage segment that would not open — the traces '
              + 'it held are missing from the list entirely, and narrowing the time window will '
              + 'NOT bring them back. The message above the filter bar says so in full.'
              : 'Part of this window sits in a storage segment the search ran out of room to '
              + 'open before it had to move on — the traces it holds are missing from the list '
              + 'entirely. Narrow the time window to bring them back into reach.')
          : 'The rows below are a prefix of the answer. The message above the filter bar is the '
          + 'server\'s own account of why it is short.',
      };
    }

    switch (end?.kind) {
      case 'capped':
        return {
          text:  `· newest ${this.listMax()} — narrow the range or the query for older traces`,
          title: 'The search stopped at the row ceiling it was given. Older traces inside this '
               + 'window exist and are not in the list.',
        };
      case 'short':
        return {
          text:  '· partial list — the server stopped before the end of the window',
          title: 'The server reported that it did not read the whole window out, for a reason '
               + 'this page does not recognise. The rows below are a prefix of the answer.',
        };
      default:
        return null;
    }
  });
  /** True when the last stream was cut short — Stop, or leaving the list mid-flight. The rows
   *  on screen are then a PREFIX of the answer, and the header has to say so: stopped at 300
   *  of a window holding 2000 must not read like a search that genuinely found 300. */
  readonly streamStopped = signal(false);
  /** The list is the user's, not the poll's: they stopped it, or a search THEY ran died and
   *  left them the rows it found plus the banner explaining why that is all there is. Either
   *  way the 15 s refresh leaves both alone until the user runs something — taking the rows
   *  back mid-read is the same theft whichever way the stream ended.
   *
   *  Both halves are things the user did and can see. A BACKGROUND refresh that fails is
   *  neither, and deliberately does not reach here: it would freeze the list over a query the
   *  user never ran, with nothing on screen saying so. It backs off instead (nextBgListAt) and
   *  says so in the list header (bgRefreshFailing).
   *
   *  The failure half reads the BANNER rather than a flag of its own, so the reason on screen
   *  and the reason the list is frozen cannot drift apart: whatever clears the banner is exactly
   *  what thaws the list.
   *
   *  A LOSS IS NOT ONE OF THE HALVES, and it was, which cost the page its refresh. Both halves
   *  above are things the USER did — they stopped the stream, or a search they ran ended badly —
   *  and holding protects that from being overwritten unread. A lost or skipped segment is neither:
   *  it is a fact about the storage, and freezing the list over it stopped the rows updating for
   *  the life of the page with nothing on screen saying so, because the staleness hint is itself
   *  suppressed while held and the failure counter cannot grow behind an early return. It bit
   *  hardest where it mattered least: `unread-segment` is not even permanent — the server's own
   *  sentence for it is that the search ran out of room before it had to move on, which the very
   *  next refresh may well get past.
   *
   *  Nothing is lost by letting the list stay live. A permanent hole is re-reported by the server
   *  on every request — that is exactly what the engine-level memory of vanished segments
   *  guarantees — so the banner comes back on its own for as long as it is true, and stops when it
   *  stops being true. That is a better contract than a page that froze once and could not tell
   *  the difference afterwards.
   *
   *  The reason this could be demoted now and not before: the treatment used to have to be decided
   *  from an English sentence, because only the `done` road carried a machine-readable cause. Both
   *  roads carry it now (see `truncatedBy` on the error frame), so one fault gets one treatment
   *  whichever frame it arrived on — which is also what stopped the same dead file rendering as a
   *  blocking banner or a grey suffix depending on how many rows fitted above it. */
  private readonly listHeld = computed(() =>
    this.streamStopped() || this.traceqlError() !== '');
  /** Rows one stream will deliver before the server stops — sent as `?max=` (which the server
   *  clamps to 1…5000 and enforces by ending the stream), and the number the header names.
   *
   *  It bounds the ARRAY, not the DOM. A trace row is 13 elements before its second service
   *  chip, so 2000 of them rendered flat is ~26k — a bill the old 200-row page cap never sent
   *  the browser. What bounds the DOM is the virtualizer below: at a 600px viewport and ~82px
   *  a row, a 2000-row answer stands at 16 rendered rows / 209 elements behind a spacer 2000
   *  rows tall (measured, traces.background-intent.spec.ts). That is also what makes the 15 s
   *  refresh affordable: re-streaming 2000 rows re-diffs 2000 array entries but touches ~200
   *  elements. Raising this costs memory and wire time, not paint. */
  readonly streamMax = 2000;
  /** Consecutive failures of the BACKGROUND refresh (the 15 s poll). A user-initiated search
   *  never lands here — it gets the banner instead. Zeroed by any complete stream and by any
   *  search the user runs, so it only ever counts an unbroken run. */
  private readonly bgFailures = signal(0);
  /** The live-refresh cadence, and the unit the background backoff is measured in. */
  private static readonly PollMs = 15_000;
  /** No two background refreshes closer together than this, whoever asked. poll() has TWO
   *  callers and only one of them is a clock: the 15 s timer paces itself, the visibility
   *  handler fires as fast as the user alt-tabs. Well under PollMs, so the scheduled cadence
   *  never trips it — this is a floor on bursts, not a second schedule. Without it, eight
   *  returns to the tab in five seconds were eight stats queries. */
  private static readonly BgFloorMs = 5_000;
  /** Earliest epoch-ms at which poll() may do any background work at all (see BgFloorMs). */
  private nextBgPollAt = 0;
  /** A load the USER asked for that the list could not run, because the list was not the panel
   *  on screen — the range chips and the filter bar render above the tab strip, so both stay
   *  live while Service Graph / Latency / Compare is showing.
   *
   *  It is what lets {@link loadAll} stop retiring a held list's warning in advance of an
   *  outcome. The old code released the hold there and then, so that returning to the tab would
   *  find an unheld list and refresh it; the cost was a banner cleared by a click that started
   *  no query. Remembering the ASK instead keeps both: the banner stands over the rows it
   *  describes until something replaces them, and {@link setMainTab} runs the load on return as
   *  what it always was — the user's own search, which empties the list and retires the marker
   *  in the same step. */
  private pendingUserLoad = false;
  /** Earliest epoch-ms at which the background LIST refresh may be attempted again after a
   *  failure. Grows 1 → 2 → 4 → 8 poll intervals with the failure count (15 s → 2 min):
   *  re-firing a doomed query every 15 s for as long as the page is open is not a retry
   *  policy.
   *
   *  A TIMESTAMP, not a countdown of ticks, and that is the whole point. A counter spent one
   *  unit per CALL to poll(), and poll() has two callers — so every hidden→visible transition
   *  burned a tick of a backoff it never waited out. Alt-tab eight times in five seconds and
   *  the ninth return re-ran the same doomed month-wide scan immediately: a backoff that made
   *  the failing query arrive FASTER than no backoff at all. Wall time cannot be spent by
   *  being asked, so it no longer matters who calls poll(), or how often.
   *
   *  Gates the list only. Stats are a different query, and the one that failed is the list's. */
  private nextBgListAt = 0;
  /** When the list last took delivery of a COMPLETE answer. Read only by bgRefreshHint(), to
   *  date the rows on screen when the refresh behind them has stopped working. */
  private readonly listLoadedAt = signal<number | null>(null);
  /** A background refresh has failed three times running (~45 s), so the list is no longer
   *  live and the user is owed that fact. Three, not one: a single dropped tick is noise.
   *  Suppressed while the list is held, where the header is already explaining itself. */
  readonly bgRefreshFailing = computed(() => this.bgFailures() >= 3 && !this.listHeld());
  /** Rows buffered before a new array is pushed into `traces`. One array per frame would
   *  re-render the whole list a thousand times over a full stream; one per 25 keeps the
   *  count visibly moving without the churn (the Events store does the same at 10). */
  private static readonly FlushEvery = 25;

  /** Refresh when the tab is re-shown after being hidden. poll() decides whether anything is
   *  actually due — this handler is one more caller, not a privileged one. */
  private _onVisibility = () => { if (!document.hidden) this.poll(); };

  // ── Computed ──────────────────────────────────────────────────────────────
  services = computed(() => {
    const set = new Set(this.traces().map(t => t.serviceName).filter(Boolean));
    return [...set].sort();
  });

  filteredTraces = computed(() => this.traces());

  /** The list spinner may only show when there is no list. The spinner and the rows are the
   *  two arms of one @if, so letting it win while rows are on screen UNMOUNTS the scroll
   *  container — and the rows come back in a brand-new element at scrollTop 0. */
  readonly listLoading = computed(() => this.loading() && this.traces().length === 0);

  // ── Virtual list ──────────────────────────────────────────────────────────
  // `.trace-rows` is the scroll element; it only exists in the rows arm of the list's @if, so
  // both queries are legitimately empty while the spinner or the empty state is showing and
  // the virtualizer takes `undefined` for its scroll element until the arm mounts.
  private readonly traceScroll = viewChild<ElementRef<HTMLElement>>('traceScroll');
  private readonly traceRowEls = viewChildren<ElementRef<HTMLElement>>('traceRowEl');

  /**
   * Renders only the rows in view. @tanstack/angular-virtual is already a dependency and
   * already drives the Events list, so this is the house pattern rather than a new one.
   *
   * MEASURED, not fixed-height, which is where it differs from Events: a trace row is a
   * timestamp, a path that wraps to a second line when the method badge crowds it, and a
   * service-chip row that wraps once per ~3 services — in a panel that itself narrows from
   * 400px to 320px when a trace is open. Events could pin 29px because its rows are one line
   * by construction; pinning a height here would mean clipping the service chips, and hiding
   * which services a trace touched to save a measurement is not a trade worth making on this
   * page. `estimateSize` is the common case (one line each) and every rendered row corrects
   * itself; the correction is a no-op once measured, since resizeItem ignores a zero delta.
   */
  readonly rowVirtualizer = injectVirtualizer(() => ({
    count:         this.filteredTraces().length,
    scrollElement: this.traceScroll(),
    estimateSize:  () => 82,
    overscan:      8,
    getItemKey:    (i: number) => this.filteredTraces()[i]?.traceId ?? i,
  }));

  constructor() {
    // Drops the row elements the virtualizer still holds by key but that are no longer in the
    // document — what measureElement(null) is for. Keyed on traceId, those entries survive the
    // answer that produced them, so without this a page left open across many searches keeps a
    // detached node alive for every trace it has ever rendered. Tracks the ROW ARRAY, not the
    // rendered elements, so scrolling does not pay for a sweep on every frame.
    afterRenderEffect(() => {
      this.filteredTraces();
      this.rowVirtualizer.measureElement(null);
    });
    // Hands each rendered row to the virtualizer to be measured and observed. afterRender, not
    // effect: the elements have to exist and be laid out before their height means anything.
    // measureElement is idempotent per node — it re-observes only when the element behind a
    // key changed, and notifies only when the height actually moved — so this cannot spin.
    afterRenderEffect(() => {
      for (const el of this.traceRowEls()) this.rowVirtualizer.measureElement(el.nativeElement);
    });
  }

  /** Services offered in the filter — backend list ∪ services seen in loaded traces. */
  serviceOptions = computed(() => {
    const set = new Set<string>(this.allServices());
    for (const s of this.services()) set.add(s);
    return [...set].sort();
  });

  // ── Waterfall computed helpers ────────────────────────────────────────────
  private traceRange = computed<{ minNs: number; totalNs: number }>(() => {
    const spans = this.traceSpans();
    if (!spans.length) return { minNs: 0, totalNs: 1 };
    let minNs = spans[0].startTimeUnixNano;
    let maxEnd = spans[0].startTimeUnixNano + spans[0].durationNanos;
    for (const s of spans) {
      if (s.startTimeUnixNano < minNs) minNs = s.startTimeUnixNano;
      const end = s.startTimeUnixNano + s.durationNanos;
      if (end > maxEnd) maxEnd = end;
    }
    return { minNs, totalNs: Math.max(1, maxEnd - minNs) };
  });

  spanDepthMap = computed(() => {
    const spans = this.traceSpans();
    const byId  = new Map(spans.map(s => [s.spanId, s]));
    const map   = new Map<string, number>();
    for (const span of spans) {
      let depth = 0, cur: SpanDto | undefined = span;
      while (cur?.parentSpanId && !isZeroId(cur.parentSpanId)) {
        cur = byId.get(cur.parentSpanId);
        if (++depth > 20) break;
      }
      map.set(span.spanId, depth);
    }
    return map;
  });

  uniqueServices = computed(() => {
    const set = new Set(this.traceSpans().map(s => s.serviceName));
    return [...set];
  });

  selectedSpanTags = computed(() => {
    const span = this.selectedSpan();
    if (!span?.attributes) return [];
    return Object.entries(span.attributes).map(([key, value]) => ({ key, value }));
  });

  // ── Lifecycle ─────────────────────────────────────────────────────────────
  ngOnInit() {
    this.restoreFromUrl();
    this.setWindow();
    this.loadAllServices();
    this.loadAll();
    this._poll = setInterval(() => this.poll(), TracesComponent.PollMs);
    document.addEventListener('visibilitychange', this._onVisibility);
  }

  /** Periodic live refresh. Skips work when the tab is hidden, and only restarts the
   *  (heavier) trace list while the Traces tab is actually on screen. */
  private poll() {
    if (document.hidden) return;
    // One clock for both callers. Everything below this line is background work, and how much
    // of it is due is a question about elapsed TIME — never about how many times poll() was
    // asked. A run of hidden→visible transitions is one refresh, not eight.
    const now = Date.now();
    if (now < this.nextBgPollAt) return;
    this.nextBgPollAt = now + TracesComponent.BgFloorMs;
    const from = this.fromIso();
    const to   = this.toIso();
    this.loadStats(from, to);
    // Stats refresh on every tick that gets this far; the list does not while a stream is
    // still delivering into it —
    // restarting mid-stream would throw away the rows already on screen and re-run the same
    // query from the top every 15 s, a list that never finishes arriving — and not while the
    // list is HELD, i.e. the user stopped it or it failed. 'background' is what makes the rest
    // of this tick invisible: nobody asked, so it may not raise a banner, may not offer Stop,
    // and — when there are rows to protect — buffers and swaps once so nothing the user is
    // reading moves under them.
    if (this.activeMainTab !== 'traces' || this.streaming() || this.listHeld()) return;
    // Back off after a background refresh fails. A held list stops polling outright because
    // the user owns it; a failing background refresh keeps trying, because the failure may be
    // a passing one (a spent budget on a busy server) and nobody asked for the list to go
    // stale — but it tries at a widening interval instead of hammering the same query.
    // The window is a deadline, so coming back to the tab does not shorten it.
    if (now < this.nextBgListAt) return;
    this.loadTraces(from, to, 'background');
  }

  private loadAllServices() {
    this.api.getServiceNames(30).subscribe({
      next: s => { this.allServices.set(s); this.cdr.markForCheck(); },
      error: () => { /* keep empty — falls back to services seen in loaded traces */ },
    });
  }

  // ── URL state sync (survives F5 / deep-link) ──────────────────────────────
  private restoreFromUrl() {
    const q = this.route.snapshot.queryParamMap;

    const tab = q.get('tab');
    if (tab === 'graph' || tab === 'latency' || tab === 'compare' || tab === 'traces')
      this.activeMainTab = tab;

    const range = q.get('range');
    if (range) this.preset = range;
    if (this.preset === 'custom') {
      this.customFrom = q.get('cfrom') ?? '';
      this.customTo   = q.get('cto')   ?? '';
    }

    const dtab = q.get('dtab');
    if (dtab === 'flamegraph' || dtab === 'details' || dtab === 'timeline')
      this.activeTraceTab = dtab;

    this.filterService    = q.get('svc')    ?? '';
    this.filterName       = q.get('name')   ?? '';
    this.filterStatus     = q.get('status') ?? '';
    this.filterHttpStatus = q.get('http')   ?? '';
    const min = q.get('min'); this.filterMinDurationMs = min ? +min : null;
    const max = q.get('max'); this.filterMaxDurationMs = max ? +max : null;

    const ql = q.get('ql');
    if (ql) { this.traceqlInput = ql; this.traceqlMode.set(true); }

    const trace = q.get('trace');
    if (trace) this.openTrace(trace);
  }

  /** Reflect the current page state into the URL query string (replaceUrl — no history spam). */
  private syncUrl() {
    const qp: Record<string, string | null> = {
      tab:    this.activeMainTab === 'traces'    ? null : this.activeMainTab,
      trace:  this.selectedTraceId() ?? null,
      dtab:   this.activeTraceTab === 'timeline' ? null : this.activeTraceTab,
      range:  this.preset === '1h'               ? null : this.preset,
      cfrom:  this.preset === 'custom' && this.customFrom ? this.customFrom : null,
      cto:    this.preset === 'custom' && this.customTo   ? this.customTo   : null,
      svc:    this.filterService    || null,
      name:   this.filterName       || null,
      status: this.filterStatus     || null,
      http:   this.filterHttpStatus || null,
      min:    this.filterMinDurationMs != null ? String(this.filterMinDurationMs) : null,
      max:    this.filterMaxDurationMs != null ? String(this.filterMaxDurationMs) : null,
      ql:     this.traceqlMode() && this.traceqlInput.trim() ? this.traceqlInput.trim() : null,
    };
    this.router.navigate([], {
      relativeTo:          this.route,
      queryParams:         qp,
      queryParamsHandling: 'merge',
      replaceUrl:          true,
    });
  }

  // ── State setters that also persist to URL ────────────────────────────────
  setMainTab(tab: 'traces' | 'graph' | 'latency' | 'compare') {
    // Selecting the tab that is ALREADY selected is not navigation, and must not be mistaken
    // for it. The template binds every tab button unconditionally, so a click on the active
    // one used to fall straight through to the "returning to the list" branch below and re-run
    // the query — which goes through startStream, whose first act is to cancel whatever is
    // streaming. A user watching 437 rows of their own month-wide search arrive lost the rest
    // of it to a click on the tab they were already on, and the replacement was a BACKGROUND
    // stream, so nothing on screen said the rows had been cut short. Nothing below has any
    // effect when the tab does not change, so the guard costs a click and closes the path.
    if (tab === this.activeMainTab) return;
    this.activeMainTab = tab;
    this.syncUrl();
    // Leaving the list cancels its stream: nobody is watching the rows land, and an open
    // EventSource holds a server connection open for a panel that is off screen.
    if (tab !== 'traces') { this.stopStream(); return; }
    // Returning refreshes it — the poll leaves the list alone while it is off screen — unless
    // the list is held. Coming back to a tab is navigation, not a new search: the rows a Stop
    // (or a failure) left behind are still the answer the user was reading, and Refresh sits
    // right next to the header that says the list is partial. 'background' for the same
    // reason: the panel is only class-hidden, never unmounted, so the list is still scrolled
    // where they left it and a glance at Service Graph must not cost them that place — nor
    // hand them a banner about a query they did not run, if the re-run happens to fail.
    //
    // Except when the user asked for a load while the list was off screen (a range chip, a
    // filter Clear — both live above the tab strip). That is not a poll tick and must not be
    // disguised as one: it outranks the hold, exactly as it would have if the list had been on
    // screen when they clicked, and it owns its own failure. Running it as the user's is also
    // what retires a held list's marker honestly — by replacing the rows the marker is about,
    // rather than by clearing it back when the click happened and hoping something followed.
    if (this.pendingUserLoad) {
      this.loadTraces(this.fromIso(), this.toIso(), 'user');
      return;
    }
    if (!this.listHeld()) this.loadTraces(this.fromIso(), this.toIso(), 'background');
  }

  setTraceTab(tab: 'timeline' | 'flamegraph' | 'details') {
    this.activeTraceTab = tab;
    this.syncUrl();
  }

  setPreset(p: string) {
    this.preset = p;
    this.syncUrl();
    // 'custom' just reveals the date inputs; the query runs on Apply.
    if (p !== 'custom') { this.setWindow(); this.loadAll(); }
  }

  /** Apply a custom range. `from` is required; `to` optional (empty → open-ended / now). */
  applyCustom() {
    if (!this.customFrom) return;
    this.syncUrl();
    this.setWindow();
    this.loadAll();
  }

  setTraceqlMode(on: boolean) {
    this.traceqlMode.set(on);
    this.traceqlError.set('');
    this.syncUrl();
  }

  toggleHistory(): void {
    this.historyOpen.update(v => !v);
  }

  /**
   * A click in the History panel — lands exactly like a hand-typed TraceQL search:
   * switch to TraceQL mode, drop the query in, clear any stale error, then run it
   * through the same user-run path (runTraceQLUser records + syncs the URL). The panel
   * itself stays open, same as the Events Signals panel.
   */
  // applyTraceql already does everything a history apply needs — including landing on the
  // Traces tab so the results render where the user can see them; the attrMenu close it
  // also performs is a harmless no-op from the panel.
  applyHistoryQuery(query: string): void {
    this.applyTraceql(query);
  }

  ngOnDestroy() {
    // Unsubscribed directly rather than through stopStream(): the view is going away, so
    // there is nothing left to flush into it and nothing left to mark for check.
    this.streamSub?.unsubscribe();
    this.streamSub = null;
    this.flushPending = null;
    this.streamPaints = false;
    if (this._poll) clearInterval(this._poll);
    document.removeEventListener('visibilitychange', this._onVisibility);
  }

  // ── Data loading ──────────────────────────────────────────────────────────
  /**
   * Every caller is a user action — Refresh, Apply, Clear, a range chip, Apply-custom, a seek
   * from the property menu — plus the initial load.
   *
   * <p>It does NOT release the held list here, and that is the fix to a warning that could be
   * retired by a click that replaced nothing. `releaseList()` used to run on this line,
   * unconditionally, ahead of a load that {@link loadTraces} may decline outright: the range
   * chips and the filter bar render ABOVE the tab strip, so they are live while Service Graph
   * is showing, and the list refuses to stream into a panel that is off screen. Measured: a
   * truncation banner up over 300 rows, one click on Service Graph, one click on a range chip
   * — and the banner was gone, the hold was gone, the same 300 truncated rows were still on
   * screen, and the background refresh that ran when the user came back could not raise a
   * banner even when it failed for the very same reason. A warning retired by a click that
   * started no query and changed no row.</p>
   *
   * <p>Releasing is now the job of the stream that makes the marker untrue — {@link startStream}
   * for a search the user ran, the `complete` handler for any stream that replaces the rows —
   * so the banner is REPLACED by the new outcome instead of being cleared in advance of it. The
   * load a hidden list could not run is remembered instead, and {@link setMainTab} runs it as
   * the user's own the moment the list is back on screen: the refresh the old release was there
   * to enable still happens, and it arrives as an answer rather than as an absence.</p>
   */
  loadAll() {
    const from = this.fromIso();
    const to   = this.toIso();
    this.loadStats(from, to);
    // Assignment, not |=: a load that DID run has already cleared the flag through startStream,
    // and a stale pending load must not survive the search that satisfied it.
    this.pendingUserLoad = !this.loadTraces(from, to);
  }

  loadStats(from: string, to?: string) {
    this.statsLoading.set(true);
    this.api.getTraceStats(from, to).subscribe({
      next: s  => { this.stats.set(s); this.statsLoading.set(false); this.cdr.markForCheck(); },
      error: () => { this.statsLoading.set(false); this.cdr.markForCheck(); },
    });
  }

  /**
   * Opens the list's stream — but only while the list is the panel on screen. The range
   * chips, the filter bar and the TraceQL box all render ABOVE the tab strip, so they stay
   * live while Service Graph / Latency / Compare is showing, and without this guard a chip
   * click there (or a deep link like /traces?tab=graph&ql=…) opened a 2000-row EventSource
   * into a hidden panel — the very connection setMainTab cancels on the way out. The stats
   * above the tabs still refresh; only the list waits, and setMainTab loads it on return.
   *
   * @returns whether the load actually ran. Its one caller that cares is {@link loadAll}, which
   *   has a held list's warning to answer for: a load that never ran cannot be what retires it.
   */
  loadTraces(from: string, to?: string, origin: LoadOrigin = 'user'): boolean {
    if (this.activeMainTab !== 'traces') return false;
    if (this.traceqlMode() && this.traceqlInput.trim()) {
      this.runTraceQL(origin);
      return true;
    }
    this.startStream(this.api.streamTraceList({
      from,
      to,
      service:        this.filterService        || undefined,
      spanName:       this.filterName           || undefined,
      status:         this.filterStatus         || undefined,
      minDurationMs:  this.filterMinDurationMs  ?? undefined,
      maxDurationMs:  this.filterMaxDurationMs  ?? undefined,
      httpStatus:     this.filterHttpStatus     || undefined,
      max:            this.streamMax,
    }), origin, this.streamMax);
    return true;
  }

  /** Reached two ways: the user pressing Run (through runTraceQLUser) and the 15 s poll,
   *  when a TraceQL query is the thing the list is currently showing — hence `origin`, which
   *  the poll's tick must carry all the way down or its failures become the user's. */
  runTraceQL(origin: LoadOrigin = 'user') {
    const q = this.traceqlInput.trim();
    if (!q) return;
    this.syncUrl();
    // The custom range's END, which this path used to drop on the floor: only `from` was
    // ever sent, so the server fell back to its "to = now" default and a query over, say,
    // last week silently ran to the present. Invisible whenever the end IS now — which is
    // why it survived — and wrong for every other custom window. The filter-bar path has
    // always sent it; TraceQL simply never did.
    this.startStream(this.api.streamTraceQuery({
      query: q, from: this.fromIso(), to: this.toIso(), max: this.streamMax,
    }), origin, this.streamMax);
  }

  /**
   * The one way a search takes over the list. Cancelling the previous stream FIRST is what
   * makes "one search owns the list" true: the old EventSource is closed before the new one
   * is even asked for, so an abandoned search has no live callback that could interleave its
   * rows with the search that replaced it.
   *
   * Whether the list is emptied here depends on WHO asked, and the two answers are opposite.
   * A background refresh must not empty it: clearing synchronously destroyed the scroll
   * container — spinner in, rows out — so the 15 s poll handed the user a fresh empty element
   * at scrollTop 0 twice a minute, whatever they were reading. A search the USER ran must
   * empty it, for the mirror-image reason: its rows answer a different question, and leaving
   * them up means the header counts and describes the PREVIOUS result while the new one
   * streams — change service=A to service=B, press Apply, and "342 traces" sits over 342 rows
   * of service A until the first B row lands, a window bounded only by the stream deadline.
   *
   * @param origin  Who asked (see {@link LoadOrigin}) — and ONLY who asked. A 'background'
   *   refresh of a list that already has rows buffers them silently and swaps in ONE array
   *   when the stream completes: paint a partial batch and the list shrinks to 1 row before
   *   growing back, and a scroll container whose content collapses is clamped to scrollTop 0
   *   by the browser — the same lost place, reached the other way. Cancelled half way it
   *   publishes NOTHING, because a half-arrived refresh is not a better answer than the
   *   complete list already on screen. A user's own search pays nothing for progressive
   *   paint: they asked for different rows, and the top is where they want to be — so it
   *   keeps flushing per batch, and so does a background refresh that finds the list EMPTY,
   *   where there is no position left to protect. That last case is why `origin` exists as a
   *   parameter rather than being inferred from the row count: an empty list makes a poll
   *   tick paint like a search, and it must still not fail like one.
   * @param askedMax  The row ceiling THIS request carried, so the header names the number that
   *   produced the rows rather than the number the page would ask for next. Passed rather than
   *   read off {@link streamMax} at render time: the two agree today and would silently stop
   *   agreeing the first time a caller asks for anything else.
   */
  private startStream(
    source: Observable<StreamFrame<TraceRowDto>>,
    origin: LoadOrigin = 'user',
    askedMax: number = this.streamMax,
  ): void {
    // A background refresh NEVER supersedes a live stream. Both of its callers already decline
    // to ask for one while a stream is open — the poll checks `streaming()`, and leaving the
    // list cancels its stream before another tab can return to it — but "no caller currently
    // does this" is not the same guarantee as "this cannot happen", and the difference cost a
    // user their search: re-selecting the active Traces tab reached here with 'background',
    // and the first thing below would have been stopStream() on 437 rows the server was still
    // sending. Nobody asked for the refresh, so it has nothing to offer that is worth ending a
    // search the user is watching arrive; the stream in flight is already the better answer.
    if (origin === 'background' && this.streamSub) return;
    this.stopStream();
    this.loading.set(true);
    this.streaming.set(true);

    // Two questions, two answers, and they are allowed to disagree.
    //
    // May I OWN THE FAILURE? — decided by who asked, and by nothing else. The row count is a
    // fact about the screen; it cannot make an unattended tick into a search the user ran.
    const userAsked = origin === 'user';
    // May I PAINT AS I GO? — decided by what is on screen, which is what the row count was
    // always for. Only a refresh that finds rows must hold them back; an empty list has no
    // scroll position to protect and nothing to lose, so the tick fills it as rows arrive.
    const silent = !userAsked && this.traces().length > 0;
    // The list exactly as this stream found it. A background refresh that fails puts it back
    // (see the error branch): for `silent` that is the array it never touched, and for the
    // empty case it is the empty list, so the prefix a dying tick painted is not left behind
    // to read as a finished answer that nothing on screen would call partial.
    const before = this.traces();

    // Stop and the "streaming…" hint are for a search the user is watching arrive. An
    // unattended tick has nothing they need to interrupt, whatever the list looked like when
    // it started.
    this.streamPublishing.set(userAsked);
    this.streamPaints = !silent;
    // Everything below belongs to a stream the USER asked for; a background refresh touches
    // none of it, which is what makes it invisible.
    if (userAsked) {
      // The old answer goes out with the question that produced it (see the doc comment) —
      // and the sentence describing it goes at the same moment, in the same branch. Retiring
      // "stopped early — partial list" and the banner is only honest for a stream that is
      // REPLACING those rows, which is exactly this one and exactly why the two lines are
      // adjacent. This used to run above, unconditionally, for every stream: stopStream()
      // stamps the partial marker on the rows it flushes, so a stream that inherited them
      // rather than producing them could rub the marker out and leave a truncated list
      // reading as a finished answer. A stream may only retire a marker it is about to make
      // untrue — and, the other way round, a stream that HAS made one untrue must retire it,
      // which is the same rule read from the other end and lives in the `complete` handler.
      // Together they are the whole guarantee: a marker stands exactly as long as the rows it
      // describes. Here is where a USER's stream keeps it, because emptying the list is the
      // next line; a background stream reaches its half later, having nothing to empty.
      this.releaseList();
      if (this.traces().length) this.traces.set([]);
      // The one place a user's load is known to have actually STARTED, so the one place a
      // remembered one is known to be satisfied. Whatever a hidden list could not run, this
      // stream is running now.
      this.pendingUserLoad = false;
      // The ending describes the list on screen, so a background refresh leaves it standing
      // until it completes with an answer of its own — otherwise "newest 2000 · narrow the
      // range" blinked out and back twice a minute under a list that never moved. The one
      // background stream that DOES replace the list — the empty-list case — cannot want this
      // either: a capped list is 2000 rows, so it is never the empty one.
      this.listEnding.set(null);
      // THE ONLY PLACE A LOSS IS EVER RETIRED, and the reason it is this place. A lost segment
      // is not a claim a later read can test — nothing re-reads a file that is gone — so no
      // stream is allowed to withdraw it by simply not mentioning it, which is exactly what a
      // background tick does after the server has been restarted or retention has passed the
      // range. What CAN retire it is the user asking again: this line runs beside the one that
      // empties the list, so the warning leaves with the rows it was about and the next answer
      // says for itself whether the hole is still there. That is also the whole recovery story
      // for a window that genuinely came back — Refresh, Apply, or any range chip.
      this.listLoss.set(null);
      // The user is driving again, so the background refresh starts from a clean slate: its
      // failure count and its backoff both describe a run of unattended ticks, and this is
      // not one. Whatever the poll could not fetch, this query is about to fetch itself.
      this.bgFailures.set(0);
      this.nextBgListAt = 0;
    }

    // Rows land in a plain array and reach the signal in batches. Pushing a new array per
    // frame would re-render the entire list once per row — 2000 renders for one search.
    //
    // `flush` is captured in a local and the frame handler calls THAT, not the field: the
    // accumulator and the publisher that empties it are then one object, paired by closure
    // rather than by the unsubscribe-before-subscribe discipline holding. It holds today; if
    // a future edit ever starts a stream without stopping the previous one, a frame from the
    // old stream reading the field would publish the NEW stream's buffer.
    const acc: TraceRowDto[] = [];
    // The server's terminal frame, held next to the rows it ended and captured in the same
    // closure for the same reason: an ending belongs to exactly one stream, and a field would
    // let a stream read the ending of the one that replaced it.
    let ended: StreamEndDto | null = null;
    const flush = () => {
      this.traces.set([...acc]);
      this.loading.set(false);
    };
    // The field means "what this stream may hand over if it ends right now" — null for a
    // silent refresh, which has nothing worth handing over until it is complete.
    this.flushPending = silent ? null : flush;

    const sub = source.subscribe({
      next: frame => {
        // The terminal frame is not a row: it is the server saying how this stream ended, and
        // it arrives immediately before the completion that consumes it. Held rather than
        // acted on here, because what it means depends on how many rows came with it.
        if (frame.kind === 'end') { ended = frame.end; return; }
        acc.push(frame.row);
        // The FIRST row flushes immediately so the empty-state spinner is replaced the
        // moment there is anything to show; after that, one flush per batch.
        if (!silent && (acc.length === 1 || acc.length % TracesComponent.FlushEvery === 0)) {
          flush();
          this.cdr.markForCheck();
        }
      },
      complete: () => {
        // Complete: a silent refresh now HAS the whole answer, so it hands it over the same
        // way a publishing stream hands over its tail — through endStream, in one array.
        this.flushPending = flush;
        // …which is also the moment this stream owns every row on screen, whoever asked for
        // it — so the markers describing the rows it just replaced go out with them. A user's
        // search retires them up front (see above) because it empties the list up front; a
        // background stream cannot retire anything there, because until it completes it may
        // still die and hand the old rows back, and a stream that may yet restore rows must
        // not rub out their marker. Completing is exactly when that stops being possible.
        //
        // Without this, a background stream that ran to COMPLETE over a stopped list published
        // its own twelve-row answer under a header still reading "stopped early — partial
        // list" — a whole answer labelled a fragment, the mirror of the bug that moved
        // releaseList() up into the userAsked branch. Both halves go together, because both
        // are sentences about rows that are gone: the banner would otherwise explain a failed
        // search over rows that search never produced, and it is half of listHeld, so it would
        // freeze a list that is complete and current with nothing on screen saying why.
        this.releaseList();
        // …and the same moment the ending becomes a fact about the rows now on screen. The
        // server said how this stream ended; what the header may claim is decided from that,
        // not from counting rows (see endingOf).
        this.listEnding.set(this.endingOf(ended, acc.length, askedMax));
        this.listMax.set(askedMax);
        // THE MARKER BELONGS TO THE ROWS IT ARRIVED WITH, so it is set unconditionally — including
        // to null — by whichever stream published what is now on screen.
        //
        // It used to be raise-only, on the reasoning that a background tick must not retire a
        // warning it did not disprove: the server's memory of a vanished segment was a field in a
        // process, so a restart made the next tick report the window as whole while the traces were
        // still missing. That reasoning came with a hold — nothing under the banner could change,
        // so the marker always described what was on screen.
        //
        // The hold is gone, because freezing a live list over a fact about storage was a deadlock
        // with no indicator. Raise-only without it is worse than either: measured, a user search
        // ending in `unreadable-segment` over 300 rows, then one background tick returning twelve
        // fresh rows and `{complete:true, exhausted}`, leaves the page holding the server's
        // positive claim that the window was read out AND a red banner calling those twelve rows
        // partial — describing 300 rows that are no longer there.
        //
        // What replaced the old protection is the server, not the page: a permanent hole is
        // re-reported on every request now that the engine remembers vanished segments, so a real
        // loss comes back with the very next tick. A marker that outlives its rows is a lie on
        // screen immediately; a marker that waits one poll interval for the server to say it again
        // is not.
        this.listLoss.set(this.lossOf(ended));
        // The list holds a whole answer as of now, and the refresh behind it is working.
        this.listLoadedAt.set(Date.now());
        this.bgFailures.set(0);
        this.nextBgListAt = 0;
        this.endStream();
      },
      error: (err: unknown) => {
        // Two different failures land here: a TraceQL parse error, which arrives before any
        // row (as a query-error FRAME — that is why the server does not answer it with a 400
        // the browser could not read), and a query that dies halfway through on a spent time
        // budget or an unreadable segment.
        if (!userAsked) {
          // A background refresh that fails leaves the visible state EXACTLY as it found it,
          // and that is a promise about BOTH halves of what it could have touched. Its rows
          // are withheld: the buffered ones were never published, and the ones it painted into
          // an empty list go back to `before` — the complete answer on screen, even when that
          // answer was "none", beats the half one this tick managed to fetch. The banner is
          // withheld too: the user never ran this query, the rows under it are not partial,
          // and the banner is half of listHeld, so planting it would also freeze the list with
          // nothing on screen saying so. What they do learn is that the list stopped being
          // live — said in the list header, next to the count, once the failure has repeated
          // (see bgRefreshFailing), and phrased as the age of the rows rather than a claim
          // about them.
          this.flushPending = null;
          this.traces.set(before);
          this.bgFailures.update(n => n + 1);
          this.nextBgListAt = Date.now()
            + TracesComponent.PollMs * Math.min(8, 2 ** (this.bgFailures() - 1));
        } else {
          // A search the user ran keeps whatever arrived and gets the reason beside it — the
          // half-answer plus its explanation, not an empty list and a banner.
          //
          // WHICH OF THE TWO IT IS, decided by the cause the server named rather than by the
          // frame the news came out on. A lost segment can end a stream either way — as this
          // error frame, or as a done frame carrying truncatedBy — and the two used to get
          // different screens: a blocking banner over a frozen list one way, a grey count suffix
          // over a live one the other, chosen by nothing more principled than how many rows
          // happened to fit above the loss.
          //
          // So a named loss is recorded AS A LOSS on this road too, and deliberately does not set
          // traceqlError: the banner is the same sentence either way (LOSS_SENTENCE is the
          // server's own), while traceqlError is what holds the list, and a fact about the
          // storage is not a reason to stop refreshing. Anything the server did NOT attribute —
          // a TraceQL parse error, a spent deadline, a dropped connection — is a failure of the
          // search itself, and that does hold: those rows are the user's half-answer and the
          // poll must not take them away unread.
          const cause = (err as { truncatedBy?: unknown })?.truncatedBy;
          if (cause === 'unread-segment' || cause === 'unreadable-segment') this.listLoss.set(cause);
          else this.traceqlError.set((err as Error)?.message?.trim() || 'Query error');
        }
        this.endStream();
      },
    });
    // Assigned AFTER subscribe returns, and only if the stream is still open. A source that
    // terminates synchronously has already run its complete/error handler — and endStream()
    // with it, which nulls this field — before subscribe() returns, so assigning the result
    // unconditionally would resurrect a finished stream as a permanently non-null `streamSub`.
    // Every "is a stream open?" test reads this field: stopStream() would stamp the list
    // partial over nothing, and the guard at the top of this method would refuse every
    // background refresh for the life of the page. Today's SSE source always goes through an
    // HTTP round trip for its ticket first, so it cannot terminate synchronously — this is the
    // field keeping its meaning rather than a bug being fixed.
    if (!sub.closed) this.streamSub = sub;
  }

  /**
   * Stops the active stream and keeps every row it already delivered. Reached from the
   * header's Stop button, from leaving the Traces tab, and from the start of the next search.
   */
  stopStream(): void {
    // Only a stream that was PAINTING leaves a partial list behind, and only then may the
    // header call it partial: called with nothing open (a second Stop, leaving the tab after
    // the stream finished) or over a silent refresh that never touched the rows, this must
    // not stamp "stopped early" on a list that is complete. `streamPaints`, not
    // `streamPublishing`: a background refresh of an EMPTY list does put rows on screen while
    // offering no Stop, and cancelling it half way still leaves a prefix that has to say so.
    if (this.streamSub && this.streamPaints) this.streamStopped.set(true);
    this.streamSub?.unsubscribe();
    this.endStream();
  }

  /** Settles the list after a stream ends, however it ended: the partial final batch is
   *  flushed here so the last rows are never stranded in the buffer. */
  private endStream(): void {
    this.flushPending?.();
    this.flushPending = null;
    this.streamSub    = null;
    this.streamPaints = false;
    this.streaming.set(false);
    this.streamPublishing.set(false);
    this.loading.set(false);
    this.cdr.markForCheck();
  }

  /** Hands the list back to the poll by dropping the two markers a NEW READ can disprove — a
   *  Stop, and a failed stream's banner. Clearing the banner here is not cosmetic: it IS part of
   *  the hold.
   *
   *  Called at the two moments such a marker stops being true, which are the two moments a
   *  stream takes the described rows away: when a search the USER ran is about to empty the
   *  list, and when any stream COMPLETES and publishes an answer of its own. Never anywhere
   *  else — a marker outlives everything except the rows it is about.
   *
   *  IT DOES NOT TOUCH {@link listLoss}, and the omission is the guard. Both markers here are
   *  statements about ONE READ: "the user stopped this one" and "this one died", each replaced
   *  wholesale by the next read's account of itself. A lost segment is a statement about the
   *  storage, which no read re-examines, so a stream that merely fails to mention it has
   *  disproved nothing — and this method is reached by background streams, which is how a poll
   *  tick after a server restart came to retire a permanent hole. Its retirement lives in
   *  {@link startStream}, on the user's branch only. */
  private releaseList(): void {
    this.streamStopped.set(false);
    this.traceqlError.set('');
  }

  /**
   * Turns the server's terminal `done` payload into the one thing the header needs from it.
   *
   * <p>Two rules, and the second is the one that matters. FIRST: when the server used a word
   * this client knows, that word decides — `max-rows` is the ceiling (plus whatever
   * `truncatedBy` names), `complete: true` is the whole window read out. SECOND: when it did
   * not, the answer degrades to what this page could always work out for itself — its own row
   * count against the ceiling it asked for — and NEVER to "complete".</p>
   *
   * <p>That second rule is why the row count survives at all. Both log streams still end with a
   * bare <c>data: {}</c>, and a future server may say something new; reading an unrecognised
   * ending as an endorsement would turn every such frame into a silent claim that the list is
   * whole. So an unknown ending can still be capped (the count says so), and an ending that
   * explicitly denies completeness is reported as short even when this client cannot say why —
   * a reason it cannot name is not a reason to repeat the server's denial back as consent.</p>
   *
   * <p>The CEILING AXIS ONLY — whether anything was lost on the way is {@link lossOf}, kept
   * apart for the reason {@link ListEnding} gives.</p>
   */
  private endingOf(end: StreamEndDto | null, rows: number, askedMax: number): ListEnding | null {
    if (end?.reason === 'max-rows') return { kind: 'capped' };
    if (end?.complete === true)     return { kind: 'read-out' };
    if (rows >= askedMax)           return { kind: 'capped' };
    if (end?.complete === false)    return { kind: 'short' };
    // An ending that said nothing this client can use, over a list short of the ceiling. Not
    // `read-out`: that is the SERVER's claim to make, and it did not make it here.
    return null;
  }

  /**
   * The other half of the terminal frame: whether part of the window is missing from the answer.
   *
   * <p>An unrecognised `truncatedBy` reads as no loss, which is the same rule the ending obeys
   * and for the same reason — saying less is available, saying something else is not. It is a
   * genuine under-report and worth naming: a future server that invents a third cause would go
   * unmentioned here until this page learns the word. The alternative is a loud, standing,
   * list-freezing warning whose sentence this page would have to make up, over a fault it cannot
   * describe, and that is the worse of the two.</p>
   */
  private lossOf(end: StreamEndDto | null): SegmentLoss | null {
    const by = end?.truncatedBy;
    return by === 'unread-segment' || by === 'unreadable-segment' ? by : null;
  }

  /** The TraceQL box's ✕: drops the query, the results it produced, and the `?ql=` in the
   *  URL — which the old version left behind, so F5 resurrected the query just cleared. */
  clearTraceql(): void {
    this.traceqlInput = '';
    this.stopStream();
    this.traces.set([]);
    this.listEnding.set(null);
    // The ✕ empties the list, so it is a user action of exactly the kind that may retire a loss
    // — the rows the warning was about are gone in the same step, and the next load (the
    // filter-bar path, with no query) reports the hole again if it is still there.
    this.listLoss.set(null);
    // Nothing on screen for a stale "rows from 14:32" to be about, and the next tick is a
    // fresh start rather than the continuation of a failing run.
    this.listLoadedAt.set(null);
    this.bgFailures.set(0);
    this.nextBgListAt = 0;
    // An emptied list is nobody's to keep: the next tick may refill it (with no query, the
    // filter-bar path), which is what clearing the box asks for. Also drops the banner.
    this.releaseList();
    this.syncUrl();
  }

  /**
   * The only two user-initiated ways to run a TraceQL query (Run button, Enter in the
   * box) — wired here instead of inside runTraceQL() itself, which is ALSO the 15 s
   * live-poll path (loadTraces → runTraceQL when a query is already active) and the
   * ?ql= deep-link auto-run path (restoreFromUrl → openTrace doesn't touch it, but the
   * initial loadAll does). Recording inside runTraceQL() would re-record on every poll
   * tick and every cross-page jump that lands here with ?ql= set.
   */
  runTraceQLUser(): void {
    if (!this.traceqlInput.trim()) return;
    this.history.record(this.traceqlInput);
    // The box renders above the tab strip, so Run can be pressed with Service Graph on
    // screen — and loadTraces refuses to stream into a hidden list, which would make the
    // button look dead. Take the user to the results instead; runTraceQL's own syncUrl()
    // then writes the tab it just landed on.
    this.activeMainTab = 'traces';
    // A search the user asked for outranks whatever was holding the list.
    this.releaseList();
    this.runTraceQL();
  }

  applyFilters() {
    const q = this.synthesizeTraceql();
    if (q) this.history.record(q);
    // Same reason Run switches tabs: the filter bar renders above the tab strip, so Apply is
    // clickable with Service Graph on screen, and loadTraces refuses to stream into a hidden
    // list. A search the user ran has to land where its results are.
    this.activeMainTab = 'traces';
    this.syncUrl();
    this.loadAll();
  }

  /** The filter bar's Clear: blank every field and re-run. Deliberately NOT applyFilters() —
   *  a reset is not a request to see results, so it does not drag the user off the panel they
   *  are on. (Also why it is a method: this was eight assignments in a template expression.) */
  clearFilters() {
    this.filterName          = '';
    this.filterService       = '';
    this.filterStatus        = '';
    this.filterMinDurationMs = null;
    this.filterMaxDurationMs = null;
    this.filterHttpStatus    = '';
    this.syncUrl();
    this.loadAll();
  }

  /**
   * Builds the TraceQL predicate the populated filter-bar fields would mean, so a
   * filter-mode Apply can be recorded in the same TraceQL vocabulary the history panel
   * (and the manual TraceQL box) already speaks — same grammar tqlPredicate below
   * targets (TraceQLParser.BuildIntrinsicPredicate / BuildAttrPredicate). Field order
   * follows the bar's own left-to-right layout; empty fields are skipped, and '' comes
   * back when nothing is set — a plain Clear (which blanks every field before calling
   * applyFilters) records nothing.
   */
  private synthesizeTraceql(): string {
    // Two fields have no honest TraceQL form, and their treatment is ALL-or-nothing: the
    // span-name box matches by case-insensitive SUBSTRING server-side while the grammar's
    // `=` is exact, and the HTTP bucket picks ('2xx'/'4xx'/'5xx') have no single-value
    // form. Dropping just the offending field from a COMBINED search would record a
    // strictly WIDER query than the one that ran — replayed, it silently returns more
    // than the user saw. A search touching either field is therefore not recorded at
    // all: no entry beats an entry that lies about what it will find, in either direction.
    if (this.filterName) return '';
    if (this.filterHttpStatus && !/^d+$/.test(this.filterHttpStatus)) return '';

    const parts: string[] = [];
    if (this.filterService) parts.push(this.tqlPredicate('service', this.filterService, false));
    if (this.filterStatus)  parts.push(`status = ${this.filterStatus.toLowerCase()}`);
    if (this.filterMinDurationMs != null) parts.push(`duration >= ${this.filterMinDurationMs}ms`);
    if (this.filterMaxDurationMs != null) parts.push(`duration <= ${this.filterMaxDurationMs}ms`);
    if (this.filterHttpStatus) parts.push(this.tqlPredicate('.http.status_code', this.filterHttpStatus, false));
    return parts.length ? `{ ${parts.join(' && ')} }` : '';
  }

  toggleTraceQL() {
    this.traceqlMode.update(v => !v);
    this.traceqlError.set('');
    this.syncUrl();
  }

  jumpToTrace() {
    const id = this.traceIdInput.trim();
    if (!id) return;
    this.openTrace(id);
  }

  openTrace(traceId: string) {
    if (this.selectedTraceId() === traceId) return;
    this.selectedTraceId.set(traceId);
    this.selectedSpan.set(null);
    this.resetLogs();
    this.syncUrl();
    this.traceLoading.set(true);
    this.api.getTrace(traceId).subscribe({
      next: spans => {
        this.traceSpans.set(spans.sort((a, b) => a.startTimeUnixNano - b.startTimeUnixNano));
        this.traceLoading.set(false);
        this.cdr.markForCheck();
      },
      error: () => { this.traceLoading.set(false); this.cdr.markForCheck(); },
    });
  }

  closeTrace() {
    this.selectedTraceId.set(null);
    this.traceSpans.set([]);
    this.selectedSpan.set(null);
    this.resetLogs();
    this.syncUrl();
  }

  /** Brief ✓ feedback after the trace id is copied from the detail header. */
  readonly traceIdCopied = signal(false);

  async copyTraceId(): Promise<void> {
    const id = this.selectedTraceId();
    if (!id) return;
    await this.copyText(id);
    this.traceIdCopied.set(true);
    setTimeout(() => { this.traceIdCopied.set(false); this.cdr.markForCheck(); }, 1500);
  }

  private async copyText(text: string): Promise<void> {
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      // navigator.clipboard needs a secure context (https/localhost) and focus —
      // plain-http hosts (e.g. http://sandbox:8555) get the legacy fallback.
      const ta = document.createElement('textarea');
      ta.value = text;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      ta.remove();
    }
  }

  // ── Span property context menu (Tags table + the left-column fields) ───────

  readonly attrMenu = signal<{
    key: string;
    value: string;
    /** TraceQL left-hand side (e.g. '.env', 'service.name', 'name'); null = copy-only. */
    tqlKey: string | null;
    /** CLEF key for the "Find in logs" cross; null hides the cross item. */
    logKey: string | null;
    /** Epoch ms when the value is a date/timestamp — enables the Seek section. */
    seekMs: number | null;
    x: number;
    y: number;
  } | null>(null);

  /** Menu for an arbitrary span attribute (Tags table): searchable + logs cross. */
  openAttrMenu(ev: MouseEvent, key: string, value: unknown): void {
    const s = String(value);
    this.openMenuAt(ev, { key, value: s, tqlKey: `.${key}`, logKey: key, seekMs: parseDateMs(s) });
  }

  /**
   * Menu for a left-column span field. `tqlKey` null = copy-only (e.g. Span ID,
   * Parent, Duration); `logKey` null = no logs cross; `seekMs` set for timestamps.
   */
  openFieldMenu(ev: MouseEvent, key: string, value: unknown, tqlKey: string | null, logKey: string | null, seekMs: number | null = null): void {
    this.openMenuAt(ev, { key, value: String(value), tqlKey, logKey, seekMs });
  }

  private openMenuAt(ev: MouseEvent, m: { key: string; value: string; tqlKey: string | null; logKey: string | null; seekMs: number | null }): void {
    ev.stopPropagation();
    const x = Math.min(ev.clientX, window.innerWidth - 240);
    const y = Math.min(ev.clientY, window.innerHeight - 290);
    this.attrMenu.set({ ...m, x, y });
  }

  /** Seek the time range to the field's timestamp ± N seconds. */
  attrSeek(seconds: number): void {
    const m = this.attrMenu();
    if (m?.seekMs == null) return;
    this.attrMenu.set(null);
    this.preset = 'custom';
    this.customFrom = msToLocal(m.seekMs - seconds * 1000);
    this.customTo   = msToLocal(m.seekMs + seconds * 1000);
    this.syncUrl();
    this.setWindow();
    this.loadAll();
  }

  @HostListener('document:click')
  closeAttrMenu(): void {
    if (this.attrMenu()) this.attrMenu.set(null);
  }

  async attrCopy(what: 'value' | 'key'): Promise<void> {
    const m = this.attrMenu();
    if (!m) return;
    await this.copyText(what === 'value' ? m.value : m.key);
    this.attrMenu.set(null);
  }

  /** Replace the query: search traces by this field alone. */
  attrFind(neq: boolean): void {
    const m = this.attrMenu();
    if (!m?.tqlKey) return;
    this.applyTraceql(`{ ${this.tqlPredicate(m.tqlKey, m.value, neq)} }`);
  }

  /** Append to the current query with && / ||. */
  attrExpr(joiner: '&&' | '||', neq: boolean): void {
    const m = this.attrMenu();
    if (!m?.tqlKey) return;
    const pred = this.tqlPredicate(m.tqlKey, m.value, neq);
    let inner = '';
    if (this.traceqlMode()) {
      const q = this.traceqlInput.trim();
      const braced = q.match(/^\{([\s\S]*)\}$/);
      inner = (braced ? braced[1] : q).trim();
    }
    this.applyTraceql(inner ? `{ (${inner}) ${joiner} ${pred} }` : `{ ${pred} }`);
  }

  /** Cross-signal jump: open the Logs page filtered by the same property. */
  attrFindInLogs(): void {
    const m = this.attrMenu();
    if (!m?.logKey) return;
    this.attrMenu.set(null);
    const ident = /^[A-Za-z_][A-Za-z0-9_]*$/.test(m.logKey) ? m.logKey : `['${m.logKey}']`;
    const value = /^-?\d+(\.\d+)?$/.test(m.value) ? m.value : `'${m.value.replaceAll("'", "''")}'`;
    void this.router.navigate(['/events'], { queryParams: { filter: `${ident} = ${value}` } });
  }

  /** A log row on the Logs tab emitted a CLEF filter — open it on the Events page. */
  findInLogs(filter: string): void {
    void this.router.navigate(['/events'], { queryParams: { filter } });
  }

  /** `tqlKey` is the already-formatted TraceQL LHS (intrinsic without a dot, attribute with). */
  private tqlPredicate(tqlKey: string, value: string, neq: boolean): string {
    const op = neq ? '!=' : '=';
    const v  = /^-?\d+(\.\d+)?$/.test(value) || value === 'true' || value === 'false'
      ? value
      : `"${value.replaceAll('"', '')}"`;
    return `${tqlKey} ${op} ${v}`;
  }

  private applyTraceql(query: string): void {
    this.attrMenu.set(null);
    this.traceqlMode.set(true);
    this.traceqlError.set('');
    this.traceqlInput = query;
    this.activeMainTab = 'traces';
    this.runTraceQLUser();
  }

  selectSpan(span: SpanDto) {
    const same = this.selectedSpan()?.spanId === span.spanId;
    this.selectedSpan.set(same ? null : span);
    if (!same) this.activeSpanTab = 'tags';
  }

  /** Closes the span detail panel. */
  closeSpan(): void {
    this.selectedSpan.set(null);
  }

  /**
   * Escape closes the innermost layer: the log modal (whose own EventDetail owns Escape
   * and emits `closed` — this page listener is registered first, so it defers) → the
   * span detail panel.
   */
  @HostListener('document:keydown.escape')
  onEscape(): void {
    if (this.attrMenu()) { this.attrMenu.set(null); return; }
    if (this.historyOpen()) { this.historyOpen.set(false); return; }
    if (this.logModalEvent()) return;
    if (this.selectedSpan()) this.selectedSpan.set(null);
  }

  private resetLogs() {
    this.traceLogs.set([]);
    this.traceLogsLoaded.set(false);
    this.onlyThisSpan.set(false);
  }

  /** Opens the Logs tab and lazily loads all logs for the trace (once). */
  openSpanLogs() {
    this.activeSpanTab = 'logs';
    if (this.traceLogsLoaded() || this.traceLogsLoading()) return;

    const traceId = this.selectedTraceId();
    const spans   = this.traceSpans();
    if (!traceId || !spans.length) return;

    // Bound the query to the trace's own time span (+ buffer) so the executor
    // can skip far segments. Logs are written by the same processes that own
    // the spans, so their @t falls inside [traceStart, traceEnd].
    let minNs = spans[0].startTimeUnixNano;
    let maxNs = spans[0].startTimeUnixNano + spans[0].durationNanos;
    for (const s of spans) {
      if (s.startTimeUnixNano < minNs) minNs = s.startTimeUnixNano;
      const end = s.startTimeUnixNano + s.durationNanos;
      if (end > maxNs) maxNs = end;
    }
    const BUF_MS = 60_000;
    const from = new Date(minNs / 1_000_000 - BUF_MS).toISOString();
    const to   = new Date(maxNs / 1_000_000 + BUF_MS).toISOString();

    this.traceLogsLoading.set(true);
    this.api.getTraceLogs(traceId, from, to).subscribe({
      next: logs => {
        this.traceLogs.set(logs);
        this.traceLogsLoaded.set(true);
        this.traceLogsLoading.set(false);
        this.cdr.markForCheck();
      },
      error: () => { this.traceLogsLoading.set(false); this.cdr.markForCheck(); },
    });
  }

  logMatchesSelectedSpan(log: EventDto): boolean {
    const sp = this.selectedSpan()?.spanId;
    return !!sp && log['@sp'] === sp;
  }

  private readonly presetHours: Record<string, number> = {
    '15m': 0.25, '30m': 0.5, '1h': 1, '3h': 3, '6h': 6, '12h': 12, '24h': 24,
  };

  /** Query lower bound — custom value, or "now − preset" (fresh each call → live list). */
  private fromIso(): string {
    if (this.preset === 'custom') return localToIso(this.customFrom);
    return formatISO(subHours(new Date(), this.presetHours[this.preset] ?? 1));
  }

  /** Query upper bound — only in custom mode; presets stay open-ended (server "now"). */
  private toIso(): string | undefined {
    return this.preset === 'custom' ? (localToIso(this.customTo) || undefined) : undefined;
  }

  /** Freezes the current window into winFrom/winTo (drives Graph/Latency). User action only. */
  private setWindow(): void {
    this.winFrom.set(this.fromIso());
    this.winTo.set(this.toIso());
  }

  // ── Waterfall helpers ─────────────────────────────────────────────────────
  waterfallLeft(span: SpanDto): number {
    const { minNs, totalNs } = this.traceRange();
    return ((span.startTimeUnixNano - minNs) / totalNs) * 100;
  }

  waterfallWidth(span: SpanDto): number {
    const { totalNs } = this.traceRange();
    return Math.max(0.3, (span.durationNanos / totalNs) * 100);
  }

  spanDepth(span: SpanDto): number {
    return this.spanDepthMap().get(span.spanId) ?? 0;
  }

  wfTimeTicks(): { pct: number; label: string }[] {
    const { totalNs } = this.traceRange();
    const totalMs = totalNs / 1_000_000;
    return Array.from({ length: 5 }, (_, i) => ({
      pct:   (i / 4) * 100,
      label: fmtMs(totalMs * i / 4),
    }));
  }

  totalDurLabel(): string {
    const { totalNs } = this.traceRange();
    return fmtMs(totalNs / 1_000_000);
  }

  traceServicesLabel(trace: TraceRowDto): string[] {
    return trace.services?.length ? trace.services : [trace.serviceName].filter(Boolean);
  }

  /**
   * What a repeatedly failing background refresh is allowed to say. It names the thing that
   * actually broke — the live refresh — and dates the rows; it says nothing about whether the
   * rows are partial, because they are not: they are the last COMPLETE answer, which is
   * exactly why they were kept. The clock is the moment of that answer, so it does not tick.
   */
  bgRefreshHint(): string {
    const ms = this.listLoadedAt();
    return ms == null
      ? '· live refresh failing — this list is not updating'
      : `· live refresh failing — rows as of ${format(new Date(ms), 'HH:mm:ss')}`;
  }

  // ── Formatting ────────────────────────────────────────────────────────────
  fmtTraceTime(nanos: number): string {
    return format(new Date(Math.round(nanos / 1_000_000)), 'dd/MM/yyyy HH:mm:ss.SSS');
  }

  fmtDurNs(nanos: number): string {
    return fmtMs(nanos / 1_000_000);
  }

  statusCls(status: string): string {
    return status === 'Error' ? 'error' : 'ok';
  }

  statusBadgeLabel(code: number | null, status: string): string {
    if (code) return code >= 400 ? `${code} ERROR` : `${code} OK`;
    return status === 'Error' ? 'Error' : status === 'Ok' ? 'OK' : status;
  }

  statusBadgeCls(code: number | null, status: string): string {
    if (status === 'Error' || (code != null && code >= 500)) return 'badge-error';
    if (code != null && code >= 400) return 'badge-warn';
    return 'badge-ok';
  }

  /** Stable per-service colour (shared hash palette — consistent with Logs / Stats). */
  svcColor(name: string): string {
    return serviceColor(name);
  }

  sparkline(data: number[]): string {
    if (!data?.length || data.length < 2) return '';
    const max = Math.max(...data, 1);
    const W = 80, H = 28;
    return data.map((v, i) => `${(i / (data.length - 1)) * W},${H - (v / max) * H}`).join(' ');
  }

  fmtStat(v: number, unit: 'count' | 'pct' | 'ms' | 'rps'): string {
    if (unit === 'ms')  return v < 1 ? '<1ms' : v >= 1000 ? `${(v / 1000).toFixed(2)}s` : `${Math.round(v)}ms`;
    if (unit === 'pct') return `${v.toFixed(2)}%`;
    if (unit === 'rps') {
      if (v >= 1)      return `${v.toFixed(1)} rps`;
      const tpm = v * 60;
      if (tpm >= 1)    return `${tpm.toFixed(1)}/min`;
      const tph = v * 3600;
      if (tph >= 1)    return `${Math.round(tph)}/h`;
      return '<1/h';
    }
    if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
    if (v >= 1_000)     return `${(v / 1_000).toFixed(1)}K`;
    return String(Math.round(v));
  }

  logLevel(log: EventDto): string {
    return log['@l'] ?? 'Information';
  }

  logMessage(log: EventDto): string {
    return log['@mt'] ?? '';
  }

  logTs(log: EventDto): string {
    // `@t` is the ISO string the server sent; the old `typeof` fork existed only because the
    // model claimed it might be a Date.
    const t = log['@t'];
    return t ? t.substring(11, 23) : '';
  }

  logLevelCls(log: EventDto): string {
    const l = this.logLevel(log).toLowerCase();
    if (l === 'error' || l === 'fatal') return 'lvl-error';
    if (l === 'warning' || l === 'warn') return 'lvl-warn';
    if (l === 'debug' || l === 'verbose') return 'lvl-debug';
    return 'lvl-info';
  }
}

function isZeroId(id: string): boolean {
  return !id || /^0+$/.test(id);
}

/** datetime-local ("yyyy-MM-ddTHH:mm") → ISO-8601, or '' when empty/invalid. */
function localToIso(local: string): string {
  if (!local) return '';
  const d = new Date(local);
  return isNaN(d.getTime()) ? '' : d.toISOString();
}

/** Recognises an ISO-8601-ish date string and returns its epoch ms, else null. */
function parseDateMs(v: string): number | null {
  if (!/^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}/.test(v)) return null;
  const ms = Date.parse(v);
  return isNaN(ms) ? null : ms;
}

/** epoch ms → "yyyy-MM-ddTHH:mm:ss" in LOCAL time (datetime-local with seconds). */
function msToLocal(ms: number): string {
  const d = new Date(ms - new Date(ms).getTimezoneOffset() * 60_000);
  return d.toISOString().slice(0, 19);
}

function fmtMs(ms: number): string {
  if (ms < 0.001) return `${(ms * 1_000_000).toFixed(0)}ns`;
  if (ms < 1)     return `${(ms * 1_000).toFixed(0)}µs`;
  if (ms < 1_000) return `${ms.toFixed(2)}ms`;
  return `${(ms / 1_000).toFixed(3)}s`;
}
