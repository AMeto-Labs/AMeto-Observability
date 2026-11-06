import { ChangeDetectionStrategy, Component, computed, inject, signal } from '@angular/core';

import { BottomSheetComponent } from '../../../../shared/components/bottom-sheet/bottom-sheet';
import {
  DEFAULT_PRESET, QUICK_RANGES, TimePreset, fmtPoint, fromDateTimeLocal, parseCustomDate, quickRangeStart,
  toDateTimeLocal, utcOffsetLabel,
} from '../../../../shared/utils/time-range';
import { StatsStore } from '../../stats.store';
import { bucketSecondsFor, fmtBucket } from '../../stats-range.util';

/**
 * The phone's time range sheet (design canvas, "Телефон — лист диапазона"): the six quick ranges
 * as chips, then From / To for a custom range, Reset and Apply. A draft until Apply, as the Logs
 * filter sheet's range is; Reset goes back to the default range and closes, as the popover's does.
 */
@Component({
  selector: 'app-stats-range-sheet',
  imports: [BottomSheetComponent],
  template: `
    <app-bottom-sheet label="Time range" (closed)="store.closeSheet()">
      <div class="bs-head">
        <h2 class="bs-title">Time range</h2>
        <span class="bs-note">{{ summary() }}</span>
      </div>

      <div class="bs-scroll rs-body">
        <section>
          <div class="bs-label">Quick ranges</div>
          <div class="bs-chips">
            @for (r of quickRanges; track r) {
              <button type="button" class="bs-chip" [class.active]="preset() === r"
                      [attr.aria-pressed]="preset() === r" (click)="pick(r)">{{ r }}</button>
            }
            <button type="button" class="bs-chip" [class.active]="preset() === 'custom'"
                    [attr.aria-pressed]="preset() === 'custom'" (click)="pick('custom')">Custom…</button>
          </div>
        </section>

        <section>
          <div class="bs-label">Custom range<span class="bs-label-note">times in your zone · {{ tz }}</span></div>
          <div class="rs-range">
            <label class="bs-field" [class.invalid]="!valid()">
              <span class="bs-field-k">From</span>
              <input type="datetime-local" class="bs-field-in" aria-label="From"
                     [value]="from()" (input)="setFrom($any($event.target).value)" />
            </label>
            <label class="bs-field">
              <span class="bs-field-k">To</span>
              <input type="datetime-local" class="bs-field-in" aria-label="To, empty for now"
                     [value]="to()" (input)="setTo($any($event.target).value)" />
            </label>
          </div>
          <p class="rs-hint">{{ bucketHint() }}</p>
        </section>
      </div>

      <div class="bs-foot">
        <button type="button" class="bs-foot-clear" (click)="reset()">Reset</button>
        <button type="button" class="bs-foot-apply" [disabled]="!valid()" (click)="apply()">Apply</button>
      </div>
    </app-bottom-sheet>
  `,
  styles: `
    :host { display: contents; }
    .rs-body { padding-top: 0; padding-bottom: 14px; }
    .rs-range { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }
    .bs-field.invalid { border-color: var(--lvl-e); }
    .rs-hint { margin: 6px 0 0; font-size: var(--fs-xs); color: var(--txt-dim); }
  `,
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class StatsRangeSheetComponent {
  readonly store = inject(StatsStore);
  readonly quickRanges = QUICK_RANGES;
  readonly tz = utcOffsetLabel();

  // ── The draft ───────────────────────────────────────────────────────
  readonly preset = signal<TimePreset>(this.store.preset());
  readonly from = signal(toDateTimeLocal(this.store.customFrom()));
  readonly to = signal(toDateTimeLocal(this.store.customTo()));

  /** The window the draft would load: a quick range resolved now, or the custom bounds. */
  readonly summary = computed(() => {
    const now = new Date();
    const p = this.preset();
    if (p !== 'custom') return `${fmtPoint(quickRangeStart(p, now), now)} – now`;
    const from = parseCustomDate(fromDateTimeLocal(this.from()));
    const to = parseCustomDate(fromDateTimeLocal(this.to()));
    if (!from) return 'Pick a start';
    return `${fmtPoint(from, now)} – ${to ? fmtPoint(to, now) : 'now'}`;
  });

  /** A custom window needs a start, and an end after it. */
  readonly valid = computed(() => {
    if (this.preset() !== 'custom') return true;
    const from = parseCustomDate(fromDateTimeLocal(this.from()));
    const toRaw = fromDateTimeLocal(this.to());
    const to = toRaw ? parseCustomDate(toRaw) : null;
    return !!from && (!toRaw || (!!to && to > from));
  });

  /** What the chart will group the draft into: `3 months = 92 buckets of 1 day`. */
  readonly bucketHint = computed(() => {
    const now = new Date();
    const p = this.preset();
    let from: Date | null;
    let to = now;
    if (p === 'custom') {
      from = parseCustomDate(fromDateTimeLocal(this.from()));
      const end = parseCustomDate(fromDateTimeLocal(this.to()));
      if (end) to = end;
    } else {
      from = quickRangeStart(p, now);
    }
    if (!from || to <= from) return 'The chart groups the range into whole buckets of a round step.';
    const seconds = bucketSecondsFor(to.getTime() - from.getTime());
    const buckets = Math.ceil((to.getTime() - from.getTime()) / (seconds * 1000));
    return `The chart groups this range into about ${buckets} buckets of ${fmtBucket(seconds)}.`;
  });

  pick(p: TimePreset): void {
    this.preset.set(p);
    if (p === 'custom' && !this.from()) {
      // Custom starts from where the window is now, so the fields are not blank.
      const w = this.store.window();
      if (w) this.from.set(toDateTimeLocal(fmtLocal(new Date(w.fromMs))));
    }
  }

  setFrom(v: string): void { this.from.set(v); this.preset.set('custom'); }
  setTo(v: string): void { this.to.set(v); this.preset.set('custom'); }

  reset(): void {
    this.store.setPreset(DEFAULT_PRESET);
    this.store.closeSheet();
  }

  apply(): void {
    if (!this.valid()) return;
    if (this.preset() === 'custom') this.store.applyRange(fromDateTimeLocal(this.from()), fromDateTimeLocal(this.to()));
    else this.store.setPreset(this.preset());
    this.store.closeSheet();
  }
}

/** `yyyy-MM-dd HH:mm` in local time, the stores' format. */
function fmtLocal(d: Date): string {
  const p = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`;
}
