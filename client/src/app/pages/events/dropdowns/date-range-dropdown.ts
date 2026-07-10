import { ChangeDetectionStrategy, Component, inject, Signal } from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';

import { DateMaskDirective } from '../../../shared/directives/date-mask.directive';
import { OverlayPanelRef } from '../../../shared/services/overlay';

export type TimePreset = '5m' | '15m' | '30m' | '1d' | '7d' | '2w' | '1mo' | 'custom';

export interface CalendarDay {
  day: number;
  month: number;
  year: number;
  isCurrentMonth: boolean;
  isToday: boolean;
  isFrom: boolean;
  isTo: boolean;
  inRange: boolean;
}

/**
 * Controller handed to the date dropdown via {@link OverlayPanelRef.data}.
 *
 * The date range shares state with the toolbar (Apply button), URL sync and
 * per-row timestamp binding, so it stays owned by the parent — this dropdown is
 * a pure view over the parent's signals + actions. Reads are signals (tracked
 * for OnPush), writes delegate straight back to the parent (single source of
 * truth, no behaviour change).
 */
export interface DateDropdownController {
  presets: { label: string; value: TimePreset }[];
  weekDays: string[];

  timePreset: Signal<TimePreset>;
  customFrom: Signal<string>;
  customTo: Signal<string>;
  fromSuggestion: Signal<string>;
  toSuggestion: Signal<string>;
  calPickingEnd: Signal<boolean>;
  monthLabel: Signal<string>;
  days: Signal<CalendarDay[]>;
  customFromValid: Signal<boolean>;
  customToValid: Signal<boolean>;
  canSearch: Signal<boolean>;

  setPreset(p: TimePreset): void;
  prevMonth(): void;
  nextMonth(): void;
  selectDay(d: CalendarDay): void;
  setFrom(v: string): void;
  setTo(v: string): void;
  setFromSuggestion(v: string): void;
  setToSuggestion(v: string): void;
  acceptFromSuggestion(): void;
  acceptToSuggestion(): void;
  suggestionLabel(s: string): string;
  onDateKeydown(e: Event): void;
  /** Commit the current range and reload events. */
  apply(): void;
}

@Component({
  selector: 'app-date-range-dropdown',
  standalone: true,
  imports: [LucideAngularModule, DateMaskDirective],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="dd-panel date-dd">
      <div class="dd-panel-header">
        <span class="dd-panel-title">Time range</span>
        <button class="dd-close-btn" title="Close" (click)="ref.close()">
          <lucide-icon name="x" [size]="13" />
        </button>
      </div>

      <div class="date-dd-body">
        <!-- Left: quick ranges — one click applies and closes -->
        <div class="date-quick">
          <div class="date-sec-label">Quick ranges</div>
          @for (p of c.presets; track p.value) {
            @if (p.value !== 'custom') {
              <button
                class="date-quick-item"
                [class.active]="c.timePreset() === p.value"
                (click)="pickPreset(p.value)">
                <span>{{ p.label }}</span>
                @if (c.timePreset() === p.value) {
                  <lucide-icon name="check" [size]="13" class="date-quick-check" />
                }
              </button>
            }
          }
        </div>

        <!-- Right: absolute range — From/To + calendar (always available) -->
        <div class="date-abs">
          <div class="date-sec-label">
            Absolute range
            @if (c.timePreset() === 'custom') { <span class="date-sec-badge">custom</span> }
          </div>

          <div class="date-abs-fields">
            <div class="date-field" [class.picking]="!c.calPickingEnd()" [class.invalid]="c.customFrom() && !c.customFromValid()">
              <span class="date-field-lbl">From</span>
              <div class="date-field-box">
                <input
                  type="text"
                  class="date-field-input"
                  placeholder="yyyy-mm-dd HH:mm"
                  [value]="c.customFrom()"
                  dateMask dateMaskType="datetime" dateMaskMode="start"
                  (valueChange)="c.setFrom($event)"
                  (suggestionChange)="c.setFromSuggestion($event)"
                  (blur)="c.setFromSuggestion('')"
                  (keydown.enter)="c.onDateKeydown($event)"
                  spellcheck="false"
                  autocomplete="off" />
              </div>
              @if (c.fromSuggestion()) {
                <button
                  class="date-field-tip"
                  tabindex="-1"
                  (mousedown)="$event.preventDefault(); c.acceptFromSuggestion()">
                  {{ c.suggestionLabel(c.fromSuggestion()) }}
                  <span class="date-field-tip-kbd">Tab ↵</span>
                </button>
              }
            </div>

            <div class="date-field" [class.picking]="c.calPickingEnd()" [class.invalid]="c.customTo() && !c.customToValid()">
              <span class="date-field-lbl">To</span>
              <div class="date-field-box">
                <input
                  type="text"
                  class="date-field-input"
                  [placeholder]="c.customTo() ? 'yyyy-mm-dd HH:mm' : 'Now'"
                  [value]="c.customTo()"
                  dateMask dateMaskType="datetime" dateMaskMode="end"
                  (valueChange)="c.setTo($event)"
                  (suggestionChange)="c.setToSuggestion($event)"
                  (blur)="c.setToSuggestion('')"
                  (keydown.enter)="c.onDateKeydown($event)"
                  spellcheck="false"
                  autocomplete="off" />
              </div>
              @if (c.toSuggestion()) {
                <button
                  class="date-field-tip"
                  tabindex="-1"
                  (mousedown)="$event.preventDefault(); c.acceptToSuggestion()">
                  {{ c.suggestionLabel(c.toSuggestion()) }}
                  <span class="date-field-tip-kbd">Tab ↵</span>
                </button>
              }
            </div>
          </div>

          <div class="date-cal">
            <div class="cal-header">
              <button class="cal-nav-btn" title="Previous month" (click)="c.prevMonth()">
                <lucide-icon name="chevron-left" [size]="14" />
              </button>
              <span class="cal-month-label">{{ c.monthLabel() }}</span>
              <button class="cal-nav-btn" title="Next month" (click)="c.nextMonth()">
                <lucide-icon name="chevron-right" [size]="14" />
              </button>
            </div>
            <div class="cal-grid">
              @for (d of c.weekDays; track d) {
                <div class="cal-dow">{{ d }}</div>
              }
              @for (day of c.days(); track day.year + '-' + day.month + '-' + day.day) {
                <button
                  class="cal-day"
                  [class.other-month]="!day.isCurrentMonth"
                  [class.today]="day.isToday"
                  [class.cal-from]="day.isFrom"
                  [class.cal-to]="day.isTo"
                  [class.cal-in-range]="day.inRange"
                  (click)="c.selectDay(day)">
                  {{ day.day }}
                </button>
              }
            </div>
          </div>
        </div>
      </div>

      <div class="dd-footer-actions">
        <button class="dd-cancel-btn" (click)="ref.close()">Cancel</button>
        <button class="dd-apply-btn" [disabled]="!c.canSearch()" (click)="apply()">Apply range</button>
      </div>
    </div>
  `,
})
export class DateRangeDropdownComponent {
  readonly ref = inject<OverlayPanelRef<void, DateDropdownController>>(OverlayPanelRef);
  readonly c = this.ref.data;

  /** Quick range: applies immediately (store reloads) and dismisses the popup. */
  pickPreset(p: TimePreset): void {
    this.c.setPreset(p);
    this.ref.close();
  }

  apply(): void {
    this.c.apply();
    this.ref.close();
  }
}
