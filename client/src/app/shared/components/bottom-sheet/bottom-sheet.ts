import { ChangeDetectionStrategy, Component, DestroyRef, inject, input, output } from '@angular/core';
import { CdkTrapFocus } from '@angular/cdk/a11y';

/**
 * A phone's bottom sheet: a scrim over the page and a panel resting on the bottom bar, with a
 * drag handle drawn on its top edge. The host projects the content and decides when the sheet
 * exists (mount it under `@if`); the sheet closes itself on the scrim and on Escape by asking
 * the host to, through {@link closed}.
 *
 * A dialog in fact as well as in name: the focus moves into the panel when it opens (the first
 * control), Tab stays inside it, and the control that opened it has the focus back when it
 * closes — the CDK focus trap, which also keeps a screen reader out of the page behind the
 * `aria-modal` scrim.
 *
 * One component for every sheet the Logs page has — filters, Signals, the message actions and
 * the property actions inside the event layer — so they share one height rule
 * (`100dvh − 48px − --bottom-nav-h`, the Traces sheet's) and one z-order above the event layer.
 * Swipe-to-dismiss is deliberately not here (out of scope, as on the Traces sheets).
 */
@Component({
  selector: 'app-bottom-sheet',
  imports: [CdkTrapFocus],
  template: `
    <div class="bs-scrim" (click)="closed.emit()"></div>
    <section class="bs-panel" role="dialog" aria-modal="true" [attr.aria-label]="label()"
             cdkTrapFocus cdkTrapFocusAutoCapture>
      <div class="bs-handle" aria-hidden="true"></div>
      <ng-content />
    </section>
  `,
  styleUrl: './bottom-sheet.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class BottomSheetComponent {
  /** The dialog's accessible name. */
  readonly label = input.required<string>();
  /** The scrim was tapped or Escape pressed: the host unmounts the sheet. */
  readonly closed = output<void>();

  constructor() {
    // Escape is read in the CAPTURE phase at the document, so the sheet — the topmost layer
    // whenever it is open — answers before the layers under it: the event detail's Escape is a
    // bubble-phase listener that takes the key for the drawer, and ran first when both listened
    // alike, closing the drawer under an open sheet. The key is taken here so that it does not.
    const onKeydown = (e: KeyboardEvent): void => {
      if (e.key !== 'Escape') return;
      e.preventDefault();
      this.closed.emit();
    };
    document.addEventListener('keydown', onKeydown, true);
    inject(DestroyRef).onDestroy(() => document.removeEventListener('keydown', onKeydown, true));
  }
}
