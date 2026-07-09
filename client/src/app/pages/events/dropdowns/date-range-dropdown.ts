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
        <span class="dd-panel-title">Date range</span>
        <button class="dd-close-btn" title="Close" (click)="ref.close()">
          <lucide-icon name="x" [size]="13" />
        </button>
      </div>

      <div class="date-dd-body">
        <!-- Left: preset list -->
        <div class="date-preset-list">
          @for (p of c.presets; track p.value) {
            <button
              class="date-preset-item"
              [class.active]="c.timePreset() === p.value"
              [class.is-custom]="p.value === 'custom'"
              (click)="c.setPreset(p.value)">
              {{ p.label }}
            </button>
          }
        </div>

        <!-- Right: calendar + inputs -->
        <div class="date-cal-panel">
          <div class="cal-header">
            <button class="cal-nav-btn" title="Previous month" (click)="c.prevMonth()">
              <lucide-icon name="chevron-left" [size]="13" />
            </button>
            <span class="cal-month-label">{{ c.monthLabel() }}</span>
            <button class="cal-nav-btn" title="Next month" (click)="c.nextMonth()">
              <lucide-icon name="chevron-right" [size]="13" />
            </button>
          </div>
          <div class="cal-pick-hint" [class.picking-end]="c.calPickingEnd()">
            {{ c.calPickingEnd() ? 'Click to set end date' : 'Click to set start date' }}
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

          <div class="cal-inputs">
            <div class="cal-input-row">
              <span class="cal-input-lbl">Start</span>
              <div class="cal-input-field-wrap">
                <div class="cal-field-wrap is-datetime" [class.invalid]="c.customFrom() && !c.customFromValid()">
                  <input
                    type="text"
                    class="cal-field-input"
                    placeholder="yyyy-mm-dd HH:mm"
                    [value]="c.customFrom()"
                    dateMask dateMaskType="datetime" dateMaskMode="start"
                    (valueChange)="c.setFrom($event)"
                    (suggestionChange)="c.setFromSuggestion($event)"
                    (blur)="c.setFromSuggestion('')"
                    (keydown.enter)="c.onDateKeydown($event)"
                    spellcheck="false"
                    autocomplete="off" />
                  <lucide-icon name="calendar" [size]="12" class="cal-field-icon" />
                </div>
                @if (c.fromSuggestion()) {
                  <button
                    class="cal-time-tip"
                    tabindex="-1"
                    (mousedown)="$event.preventDefault(); c.acceptFromSuggestion()">
                    {{ c.suggestionLabel(c.fromSuggestion()) }}
                    <span class="cal-time-tip-kbd">Tab ↵</span>
                  </button>
                }
              </div>
            </div>
            <div class="cal-input-row">
              <span class="cal-input-lbl">End</span>
              <div class="cal-input-field-wrap">
                <div class="cal-field-wrap is-datetime" [class.invalid]="c.customTo() && !c.customToValid()">
                  <input
                    type="text"
                    class="cal-field-input"
                    [placeholder]="c.customTo() ? 'yyyy-mm-dd HH:mm' : 'Now (open range)'"
                    [value]="c.customTo()"
                    dateMask dateMaskType="datetime" dateMaskMode="end"
                    (valueChange)="c.setTo($event)"
                    (suggestionChange)="c.setToSuggestion($event)"
                    (blur)="c.setToSuggestion('')"
                    (keydown.enter)="c.onDateKeydown($event)"
                    spellcheck="false"
                    autocomplete="off" />
                  <lucide-icon name="calendar" [size]="12" class="cal-field-icon" />
                </div>
                @if (c.toSuggestion()) {
                  <button
                    class="cal-time-tip"
                    tabindex="-1"
                    (mousedown)="$event.preventDefault(); c.acceptToSuggestion()">
                    {{ c.suggestionLabel(c.toSuggestion()) }}
                    <span class="cal-time-tip-kbd">Tab ↵</span>
                  </button>
                }
              </div>
            </div>
          </div>

          <div class="dd-footer-actions">
            <button class="dd-cancel-btn" (click)="ref.close()">Cancel</button>
            <button class="dd-apply-btn" [disabled]="!c.canSearch()" (click)="apply()">Apply</button>
          </div>
        </div>
      </div>
    </div>
  `,
})
export class DateRangeDropdownComponent {
  readonly ref = inject<OverlayPanelRef<void, DateDropdownController>>(OverlayPanelRef);
  readonly c = this.ref.data;

  apply(): void {
    this.c.apply();
    this.ref.close();
  }
}
