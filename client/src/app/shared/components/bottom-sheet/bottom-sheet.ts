import { ChangeDetectionStrategy, Component, input, output } from '@angular/core';

/**
 * A phone's bottom sheet: a scrim over the page and a panel resting on the bottom bar, with a
 * drag handle drawn on its top edge. The host projects the content and decides when the sheet
 * exists (mount it under `@if`); the sheet closes itself on the scrim and on Escape by asking
 * the host to, through {@link closed}.
 *
 * One component for every sheet the Logs page has — filters, Signals, the message actions and
 * the property actions inside the event layer — so they share one height rule
 * (`100dvh − 48px − --bottom-nav-h`, the Traces sheet's) and one z-order above the event layer.
 * Swipe-to-dismiss is deliberately not here (out of scope, as on the Traces sheets).
 */
@Component({
  selector: 'app-bottom-sheet',
  template: `
    <div class="bs-scrim" (click)="closed.emit()"></div>
    <section class="bs-panel" role="dialog" aria-modal="true" [attr.aria-label]="label()">
      <div class="bs-handle" aria-hidden="true"></div>
      <ng-content />
    </section>
  `,
  styleUrl: './bottom-sheet.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
  host: { '(document:keydown.escape)': 'onEscape($event)' },
})
export class BottomSheetComponent {
  /** The dialog's accessible name. */
  readonly label = input.required<string>();
  /** The scrim was tapped or Escape pressed: the host unmounts the sheet. */
  readonly closed = output<void>();

  onEscape(e: Event): void {
    // A layer under this one (the event detail) may own the key already.
    if (e.defaultPrevented) return;
    e.preventDefault();
    this.closed.emit();
  }
}
