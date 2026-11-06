import { ChangeDetectionStrategy, Component, input } from '@angular/core';

export interface BreakdownCol {
  text: string;
  muted?: boolean;
  /** A colour for a figure that should be noticed: an error share past the line. */
  color?: string;
}

/** One row of a band's breakdown: a colour dot or a kind badge, a name, a bar and two figures. */
export interface BreakdownRow {
  key: string;
  name: string;
  /** The dot's colour, and the bar's unless a badge gives its own. */
  color?: string;
  /** A one-letter badge in place of the dot (a metric's kind). */
  badge?: string;
  badgeColor?: string;
  /** The bar's length, 0–100, against the longest row; omit for no bar. */
  barPct?: number | null;
  cols: BreakdownCol[];
}

/** The column captions over a list whose figures are not self-evident. */
export interface BreakdownHeader {
  name: string;
  bar?: string;
  cols: string[];
}

/**
 * The rows of a band's breakdown card (design canvas, the Top services / Top metrics / Services
 * lists): a 24px grid row of dot-or-badge, name, bar, and two right-aligned figures, with an
 * optional 16px caption row. One component for the three sections, so the rows match.
 */
@Component({
  selector: 'app-stats-rows',
  template: `
    @if (header(); as h) {
      <div class="row hdr" aria-hidden="true">
        <span></span>
        <span>{{ h.name }}</span>
        <span>{{ h.bar ?? '' }}</span>
        @for (c of h.cols; track $index) { <span class="num">{{ c }}</span> }
      </div>
    }
    <div class="lst" role="list">
      @for (r of rows(); track r.key) {
        <div class="row" role="listitem">
          @if (r.badge) {
            <span class="kb" [style.color]="r.badgeColor" [style.background]="badgeBg(r.badgeColor)">{{ r.badge }}</span>
          } @else {
            <span class="dot" [style.background]="r.color"></span>
          }
          <span class="nm" [title]="r.name">{{ r.name }}</span>
          <span class="trk">
            @if (r.barPct != null) {
              <span class="trk-fill" [style.width.%]="r.barPct" [style.background]="r.badgeColor ?? r.color"></span>
            }
          </span>
          @for (c of r.cols; track $index) {
            <span class="num tn" [class.muted]="c.muted" [style.color]="c.color">{{ c.text }}</span>
          }
        </div>
      }
    </div>
  `,
  styleUrl: './stats-rows.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class StatsRowsComponent {
  readonly rows = input.required<BreakdownRow[]>();
  readonly header = input<BreakdownHeader | null>(null);

  badgeBg(color: string | undefined): string {
    return color ? `color-mix(in srgb, ${color} 16%, transparent)` : 'var(--surface-2)';
  }
}
