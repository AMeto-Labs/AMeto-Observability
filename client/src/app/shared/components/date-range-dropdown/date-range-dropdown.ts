import {
  ChangeDetectionStrategy, Component, ElementRef, Injector, Signal,
  afterNextRender, computed, inject, signal,
} from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';
import { format } from 'date-fns';

import { DateMaskDirective } from '../../../shared/directives/date-mask.directive';
import { OverlayPanelRef } from '../../../shared/services/overlay';
import {
  DEFAULT_PRESET, TimePreset, fmtPoint, parseCustomDate, utcOffsetLabel,
} from '../store/events-filter.util';

/**
 * Controller handed to the time range popover via {@link OverlayPanelRef.data}.
 *
 * The applied window is store-owned — it is shared with the toolbar, URL sync and per-row
 * timestamp binding — so the popover reads it here and writes it back only through
 * `setPreset` / `applyRange`. Everything typed or picked before Apply is the popover's own
 * draft, so closing it any other way leaves the page as it was.
 */
export interface DateDropdownController {
  presets: { label: string; value: TimePreset }[];
  timePreset: Signal<TimePreset>;
  /** The applied bounds, `yyyy-MM-dd HH:mm`; `customTo` '' for "up to now". */
  customFrom: Signal<string>;
  customTo: Signal<string>;
  setPreset(p: TimePreset): void;
  applyRange(from: string, to: string): void;
}

type Side = 'from' | 'to';
type Picker = { side: Side; kind: 'date' | 'time' } | null;

interface CalendarCell {
  iso: string;
  day: number;
  outside: boolean;
  today: boolean;
  selected: boolean;
  inRange: boolean;
  future: boolean;
}

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const TIME_RE = /^([01]\d|2[0-3]):[0-5]\d$/;
const HOURS = Array.from({ length: 24 }, (_, h) => String(h).padStart(2, '0'));
const MINUTES = Array.from({ length: 12 }, (_, i) => String(i * 5).padStart(2, '0'));
const WEEK_DAYS = ['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su'];

/** Where a side with no time of its own starts or ends: the whole day. */
const DAY_START = '00:00';
const DAY_END = '23:59';

/** A real calendar day: `2026-02-31` parses (to 3 March), so the round trip is the test. */
function isDate(s: string): boolean {
  if (!DATE_RE.test(s)) return false;
  const d = parseCustomDate(s);
  return !!d && format(d, 'yyyy-MM-dd') === s;
}

/** `17 Oct`, with the year only when it is not the current one. */
function fmtDay(d: Date, now: Date): string {
  return format(d, d.getFullYear() === now.getFullYear() ? 'd MMM' : 'd MMM yyyy');
}

/**
 * Time range popover: quick ranges, then From – To as a date each, with a time only when asked
 * for, then Apply.
 */
@Component({
  selector: 'app-date-range-dropdown',
  imports: [LucideAngularModule, DateMaskDirective],
  changeDetection: ChangeDetectionStrategy.OnPush,
  styleUrl: './date-range-dropdown.scss',
  template: `
    <div class="trp" role="dialog" aria-label="Time range"
      (pointerdown)="closePickerOutside($event)"
      (keydown.escape)="closePickerOnEscape($event)">
      <div class="trp-head">
        <span class="trp-title">Time range</span>
        <span class="trp-summary">{{ summary() }}</span>
      </div>

      <div class="trp-chips">
        @for (p of c.presets; track p.value) {
          <button type="button" class="trp-chip"
            [class.active]="c.timePreset() === p.value"
            [attr.aria-pressed]="c.timePreset() === p.value"
            (click)="pickPreset(p.value)">{{ p.label }}</button>
        }
      </div>

      <div class="trp-sides">
        @for (s of sides; track s.side) {
          @if (s.side === 'to') { <span class="trp-dash" aria-hidden="true">–</span> }
          <div class="trp-side">
            <label class="trp-label" [attr.for]="'trp-' + s.side + '-date'">{{ s.label }}</label>

            <!-- Date -->
            <div class="trp-anchor" [class.open]="isOpen(s.side, 'date')">
              <div class="trp-field"
                [class.active]="isOpen(s.side, 'date')"
                [class.invalid]="dateInvalid(s.side)">
                <div class="trp-box">
                  <input
                    [id]="'trp-' + s.side + '-date'"
                    type="text"
                    class="trp-input"
                    [placeholder]="s.side === 'to' ? 'Now' : 'yyyy-mm-dd'"
                    [value]="date(s.side)"
                    dateMask dateMaskType="date"
                    (valueChange)="setDate(s.side, $event)"
                    (keydown.enter)="apply()"
                    spellcheck="false"
                    autocomplete="off" />
                  @if (date(s.side)) {
                    <button type="button" class="trp-clear" [attr.aria-label]="'Clear ' + s.label"
                      (click)="clear(s.side)">
                      <lucide-icon name="x" [size]="12" />
                    </button>
                  }
                </div>
                <button type="button" class="trp-pick"
                  [attr.aria-label]="'Pick ' + s.label + ' date'"
                  [attr.aria-expanded]="isOpen(s.side, 'date')"
                  (click)="toggle(s.side, 'date')">
                  <lucide-icon name="calendar" [size]="14" />
                </button>
              </div>

              @if (isOpen(s.side, 'date')) {
                <div class="trp-pop trp-cal" role="dialog" [attr.aria-label]="'Pick ' + s.label + ' date'">
                  <div class="trp-cal-head">
                    <button type="button" class="trp-cal-nav" aria-label="Previous month" (click)="shiftMonth(-1)">
                      <lucide-icon name="chevron-left" [size]="14" />
                    </button>
                    <span class="trp-cal-month">{{ calLabel() }}</span>
                    <button type="button" class="trp-cal-nav" aria-label="Next month" (click)="shiftMonth(1)">
                      <lucide-icon name="chevron-right" [size]="14" />
                    </button>
                  </div>
                  <div class="trp-cal-grid">
                    @for (d of weekDays; track d) { <span class="trp-cal-dow">{{ d }}</span> }
                    @for (cell of calDays(); track cell.iso) {
                      <button type="button" class="trp-cal-day"
                        [class.outside]="cell.outside"
                        [class.today]="cell.today"
                        [class.selected]="cell.selected"
                        [class.in-range]="cell.inRange"
                        [class.future]="cell.future"
                        [attr.aria-pressed]="cell.selected"
                        (click)="pickDay(s.side, cell.iso)">{{ cell.day }}</button>
                    }
                  </div>
                  <div class="trp-pop-foot">
                    <button type="button" class="trp-link" (click)="pickDay(s.side, todayIso())">Today</button>
                  </div>
                </div>
              }
            </div>

            <!-- Time: only after Set times -->
            @if (timesShown()) {
              <div class="trp-anchor" [class.open]="isOpen(s.side, 'time')">
                <div class="trp-field"
                  [class.active]="isOpen(s.side, 'time')"
                  [class.invalid]="timeInvalid(s.side)">
                  <div class="trp-box">
                    <input
                      type="text"
                      class="trp-input"
                      placeholder="HH:mm"
                      [attr.aria-label]="s.label + ' time'"
                      [value]="time(s.side)"
                      dateMask dateMaskType="time"
                      (valueChange)="setTime(s.side, $event)"
                      (keydown.enter)="apply()"
                      spellcheck="false"
                      autocomplete="off" />
                  </div>
                  <button type="button" class="trp-pick"
                    [attr.aria-label]="'Pick ' + s.label + ' time'"
                    [attr.aria-expanded]="isOpen(s.side, 'time')"
                    (click)="toggle(s.side, 'time')">
                    <lucide-icon name="clock" [size]="14" />
                  </button>
                </div>

                @if (isOpen(s.side, 'time')) {
                  <div class="trp-pop trp-tp" role="dialog" [attr.aria-label]="'Pick ' + s.label + ' time'">
                    <div class="trp-tp-head">
                      <span class="trp-tp-name">{{ s.label }} time</span>
                      <span class="trp-tp-value">{{ time(s.side) || '--:--' }}</span>
                    </div>
                    <div class="trp-tp-cols">
                      <div class="trp-tp-col">
                        <span class="trp-tp-unit">HH</span>
                        <div class="trp-tp-list" role="group" aria-label="Hours">
                          @for (h of hours; track h) {
                            <button type="button" class="trp-tp-item"
                              [class.selected]="time(s.side).slice(0, 2) === h"
                              [attr.aria-pressed]="time(s.side).slice(0, 2) === h"
                              (click)="pickHour(s.side, h)">{{ h }}</button>
                          }
                        </div>
                      </div>
                      <div class="trp-tp-col">
                        <span class="trp-tp-unit">mm</span>
                        <div class="trp-tp-list" role="group" aria-label="Minutes">
                          @for (m of minutes; track m) {
                            <button type="button" class="trp-tp-item"
                              [class.selected]="time(s.side).slice(3, 5) === m"
                              [attr.aria-pressed]="time(s.side).slice(3, 5) === m"
                              (click)="pickMinute(s.side, m)">{{ m }}</button>
                          }
                        </div>
                      </div>
                    </div>
                    <div class="trp-pop-foot">
                      <button type="button" class="trp-link"
                        [title]="s.side === 'to' ? 'Up to now: leave To open' : 'The current time'"
                        (click)="pickNow(s.side)">Now</button>
                      <button type="button" class="dd-apply-btn" (click)="picker.set(null)">OK</button>
                    </div>
                  </div>
                }
              </div>
            }
          </div>
        }
      </div>

      <div class="trp-note">
        <span>Times are in your time zone · {{ tz }}</span>
        @if (timesShown()) {
          <button type="button" class="trp-link" (click)="wholeDays()">Whole days</button>
        } @else {
          <button type="button" class="trp-link trp-set-times" (click)="setTimes()">
            <lucide-icon name="clock" [size]="11" />
            Set times
          </button>
        }
      </div>

      <div class="trp-actions">
        <button type="button" class="dd-reset-btn" (click)="reset()">Reset</button>
        <button type="button" class="dd-apply-btn" [disabled]="!canApply()" (click)="apply()">Apply</button>
      </div>
    </div>
  `,
})
export class DateRangeDropdownComponent {
  readonly ref = inject<OverlayPanelRef<void, DateDropdownController>>(OverlayPanelRef);
  readonly c = this.ref.data;
  private readonly host = inject<ElementRef<HTMLElement>>(ElementRef);
  private readonly injector = inject(Injector);

  readonly sides: { side: Side; label: string }[] = [
    { side: 'from', label: 'From' },
    { side: 'to', label: 'To' },
  ];
  readonly hours = HOURS;
  readonly minutes = MINUTES;
  readonly weekDays = WEEK_DAYS;
  readonly tz = utcOffsetLabel();

  // ── Draft: what the fields hold until Apply ───────────────────────────
  private readonly appliedFrom = this.c.customFrom();
  private readonly appliedTo = this.c.customTo();
  private readonly appliedPreset = this.c.timePreset();

  readonly fromDate = signal(this.appliedFrom.split(' ')[0] ?? '');
  readonly toDate = signal(this.appliedTo.split(' ')[0] ?? '');
  /**
   * A preset's times are where the clock happened to be, not something the reader chose, so the
   * popover opens without them. A custom window brings its times along exactly as applied —
   * 00:00 and 23:59 included, or reopening would lose them.
   */
  readonly fromTime = signal(this.appliedTime(this.appliedFrom));
  readonly toTime = signal(this.appliedTime(this.appliedTo));
  /**
   * Whether the time fields are out — both sides together. They open out for a custom window
   * whose times are not just the whole day's; hidden times are kept, only not used.
   */
  readonly timesShown = signal(
    (!!this.fromTime() && this.fromTime() !== DAY_START) || (!!this.toTime() && this.toTime() !== DAY_END));

  /** Whether anything was edited — an untouched Apply under a preset re-runs the preset. */
  private readonly touched = signal(false);

  readonly picker = signal<Picker>(null);
  readonly calMonth = signal({ year: 0, month: 0 });

  /** The draft From as `yyyy-MM-dd HH:mm`; with no time shown it starts at 00:00. */
  readonly fromValue = computed(() => {
    const day = this.fromDate();
    return day ? `${day} ${this.timesShown() && this.fromTime() ? this.fromTime() : DAY_START}` : '';
  });

  /**
   * The draft To, '' for "up to now". With no time shown it runs to 23:59; a time with no date
   * of its own is on the From day — the usual "09:00 to 13:00".
   */
  readonly toValue = computed(() => {
    const time = this.timesShown() && this.toTime() ? this.toTime() : '';
    const day = this.toDate() || (time ? this.fromDate() : '');
    return day ? `${day} ${time || DAY_END}` : '';
  });

  private readonly start = computed(() =>
    !this.badDate('from') && !this.badTime('from') && this.fromDate() ? parseCustomDate(this.fromValue()) : null);

  private readonly end = computed(() => {
    const v = this.toValue();
    if (!v || this.badDate('to') || this.badTime('to')) return null;
    return isDate(v.slice(0, 10)) ? parseCustomDate(v) : null;
  });

  /** A set To that is not after From. */
  private readonly backwards = computed(() => {
    const s = this.start(), e = this.end();
    return !!s && !!e && e <= s;
  });

  readonly canApply = computed(() =>
    !!this.start() && (!this.toValue() || !!this.end()) && !this.backwards());

  /** What Apply would load: the preset as it stands until something is edited, then the draft. */
  readonly summary = computed(() => {
    const now = new Date();
    if (!this.touched() && this.appliedPreset !== 'custom') {
      const from = parseCustomDate(this.appliedFrom);
      return from ? `From ${fmtPoint(from, now)} → now` : '';
    }
    const s = this.start();
    if (!s) return 'Pick a start';
    const e = this.end();
    const from = this.timesShown() ? fmtPoint(s, now) : fmtDay(s, now);
    const to = !e ? 'now' : this.timesShown() && this.toTime() ? fmtPoint(e, now) : fmtDay(e, now);
    return `From ${from} → ${to}`;
  });

  // ── Calendar ───────────────────────────────────────────────────────────
  readonly calLabel = computed(() => {
    const { year, month } = this.calMonth();
    return format(new Date(year, month, 1), 'MMMM yyyy');
  });

  readonly calDays = computed<CalendarCell[]>(() => {
    const { year, month } = this.calMonth();
    const offset = (new Date(year, month, 1).getDay() + 6) % 7;   // Monday first
    const today = this.todayIso();
    const selected = this.picker()?.side === 'to' ? this.toDate() : this.fromDate();
    const from = isDate(this.fromDate()) ? this.fromDate() : '';
    const to = isDate(this.toDate()) ? this.toDate() : '';
    return Array.from({ length: 42 }, (_, i) => {
      const d = new Date(year, month, 1 - offset + i);
      const iso = format(d, 'yyyy-MM-dd');
      return {
        iso,
        day: d.getDate(),
        outside: d.getMonth() !== month,
        today: iso === today,
        selected: iso === selected,
        inRange: !!from && !!to && iso > from && iso < to,
        future: iso > today,
      };
    });
  });

  // ── Reads used by the template ─────────────────────────────────────────
  date(side: Side): string { return side === 'from' ? this.fromDate() : this.toDate(); }
  time(side: Side): string { return side === 'from' ? this.fromTime() : this.toTime(); }

  /** Red on the field: it does not parse, or — on To — it ends the window before it starts. */
  dateInvalid(side: Side): boolean {
    return this.badDate(side) || (side === 'to' && !!this.toDate() && this.backwards());
  }

  timeInvalid(side: Side): boolean {
    return this.badTime(side) || (side === 'to' && !!this.toTime() && this.backwards());
  }

  isOpen(side: Side, kind: 'date' | 'time'): boolean {
    const p = this.picker();
    return p?.side === side && p.kind === kind;
  }

  todayIso(): string { return format(new Date(), 'yyyy-MM-dd'); }

  // ── Edits ──────────────────────────────────────────────────────────────
  setDate(side: Side, v: string): void {
    (side === 'from' ? this.fromDate : this.toDate).set(v);
    this.touched.set(true);
  }

  setTime(side: Side, v: string): void {
    (side === 'from' ? this.fromTime : this.toTime).set(v);
    this.touched.set(true);
  }

  clear(side: Side): void {
    this.setDate(side, '');
    this.setTime(side, '');
  }

  /**
   * Set times: both time fields come out. A side with a time keeps it; one without starts at the
   * whole day's bound — From 00:00, To 23:59 — and a side with no day gets today, so the 23:59
   * has a day to end ("now" is today).
   */
  setTimes(): void {
    const today = this.todayIso();
    if (!this.fromDate()) this.fromDate.set(today);
    if (!this.toDate()) this.toDate.set(today);
    if (!TIME_RE.test(this.fromTime())) this.fromTime.set(DAY_START);
    if (!TIME_RE.test(this.toTime())) this.toTime.set(DAY_END);
    this.timesShown.set(true);
    this.touched.set(true);
  }

  toggle(side: Side, kind: 'date' | 'time'): void {
    if (this.isOpen(side, kind)) { this.picker.set(null); return; }
    if (kind === 'date') {
      const shown = parseCustomDate(isDate(this.date(side)) ? this.date(side) : this.fromDate()) ?? new Date();
      this.calMonth.set({ year: shown.getFullYear(), month: shown.getMonth() });
    }
    this.picker.set({ side, kind });
    if (kind === 'time') afterNextRender(() => this.centreSelected(), { injector: this.injector });
  }

  shiftMonth(by: -1 | 1): void {
    const { year, month } = this.calMonth();
    const d = new Date(year, month + by, 1);
    this.calMonth.set({ year: d.getFullYear(), month: d.getMonth() });
  }

  pickDay(side: Side, iso: string): void {
    this.setDate(side, iso);
    this.picker.set(null);
  }

  pickHour(side: Side, h: string): void {
    const t = this.time(side);
    this.setTime(side, `${h}:${TIME_RE.test(t) ? t.slice(3, 5) : '00'}`);
  }

  /** The minute is the last thing picked, so it closes the picker; the hour leaves it open. */
  pickMinute(side: Side, m: string): void {
    const t = this.time(side);
    this.setTime(side, `${TIME_RE.test(t) ? t.slice(0, 2) : '00'}:${m}`);
    this.picker.set(null);
  }

  /**
   * A press anywhere in the popover outside the open picker and its own field closes the picker.
   * Presses outside the popover close all of it (DropdownService); this covers the inside.
   */
  closePickerOutside(e: Event): void {
    if (!this.picker()) return;
    if (!(e.target as Element | null)?.closest('.trp-anchor.open')) this.picker.set(null);
  }

  /**
   * Esc closes an open picker and nothing else. Stopping it here keeps it from the page's
   * document-level Esc, which closes every dropdown, and from the overlay's own; with no picker
   * open it goes on and closes the popover as before.
   */
  closePickerOnEscape(e: Event): void {
    if (!this.picker()) return;
    e.stopPropagation();
    e.preventDefault();
    this.picker.set(null);
  }

  /** From: the current time (and today, when no day is set). To: open again, "up to now". */
  pickNow(side: Side): void {
    if (side === 'to') this.clear('to');
    else {
      const now = new Date();
      if (!this.fromDate()) this.fromDate.set(format(now, 'yyyy-MM-dd'));
      this.setTime('from', format(now, 'HH:mm'));
    }
    this.picker.set(null);
  }

  /**
   * Whole days on both sides: From starts at 00:00 and a set To ends at 23:59. The times are
   * hidden, not erased, so Set times brings them back as they were.
   */
  wholeDays(): void {
    this.timesShown.set(false);
    this.touched.set(true);
    if (this.picker()?.kind === 'time') this.picker.set(null);
  }

  // ── Commit ─────────────────────────────────────────────────────────────
  /** Quick range: applies immediately (the store reloads) and closes. */
  pickPreset(p: TimePreset): void {
    this.c.setPreset(p);
    this.ref.close();
  }

  reset(): void {
    this.c.setPreset(DEFAULT_PRESET);
    this.ref.close();
  }

  apply(): void {
    if (!this.canApply()) return;
    // Untouched fields under a preset are the preset itself: re-run it, so it stays relative,
    // rather than freezing it into a custom window.
    if (!this.touched() && this.appliedPreset !== 'custom') this.c.setPreset(this.appliedPreset);
    else this.c.applyRange(this.fromValue(), this.toValue());
    this.ref.close();
  }

  /** The applied side's time, or '' under a preset. */
  private appliedTime(applied: string): string {
    return this.appliedPreset === 'custom' ? applied.split(' ')[1] ?? '' : '';
  }

  /**
   * The field's own text is wrong, whatever the other side says. Kept apart from the order check
   * because `start` and `end` are built on it, and `backwards` on them.
   */
  private badDate(side: Side): boolean {
    const d = this.date(side);
    if (d) return !isDate(d);
    // From is required once a time is in it; an empty To is "up to now".
    return side === 'from' && this.timesShown() && !!this.fromTime();
  }

  private badTime(side: Side): boolean {
    const t = this.time(side);
    return this.timesShown() && !!t && !TIME_RE.test(t);
  }

  /** Scrolls each open time column so its selected value sits in the middle. */
  private centreSelected(): void {
    for (const list of this.host.nativeElement.querySelectorAll<HTMLElement>('.trp-tp-list')) {
      const sel = list.querySelector<HTMLElement>('.selected');
      if (sel) list.scrollTop = sel.offsetTop - (list.clientHeight - sel.offsetHeight) / 2;
    }
  }
}
