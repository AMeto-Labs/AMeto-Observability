import {
  ChangeDetectionStrategy,
  Component,
  booleanAttribute,
  input,
  output,
} from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';

/**
 * Lightweight centered modal dialog with a backdrop. Rendered with
 * position:fixed so it escapes any overflow-clipped ancestor. Content is
 * projected via <ng-content>. Emits `close` on backdrop click / close button
 * (unless `dismissable` is false).
 */
@Component({
  selector: 'app-modal',
  imports: [LucideAngularModule],
  templateUrl: './modal.html',
  styleUrl: './modal.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ModalComponent {
  readonly open = input<boolean>(false);
  readonly title = input<string>('');
  /** The title is its own tooltip too — for a dialog that may show less than the whole of it. */
  readonly titleTooltip = input(false, { transform: booleanAttribute });
  readonly dismissable = input(true, { transform: booleanAttribute });
  /** For a whole panel rather than a form (log details): up to 1100px wide, full height. */
  readonly wide = input(false, { transform: booleanAttribute });

  readonly close = output<void>();

  onBackdrop(): void {
    if (this.dismissable()) this.close.emit();
  }
}
