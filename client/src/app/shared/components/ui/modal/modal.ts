import { FocusTrap, FocusTrapFactory } from '@angular/cdk/a11y';
import {
  ChangeDetectionStrategy,
  Component,
  ElementRef,
  Injectable,
  Injector,
  afterNextRender,
  afterRenderEffect,
  booleanAttribute,
  inject,
  input,
  output,
  viewChild,
} from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';

/** A dialog on screen. */
interface OpenDialog {
  readonly box: HTMLElement;
  readonly trap: FocusTrap;
  /** What had the focus when the dialog came up, and has it back when the dialog goes. */
  returnTo: HTMLElement | null;
}

/**
 * The dialogs on screen, in the order they came up. The last is the one in use: it alone keeps
 * the focus and answers Escape. One may stand over another — a value opened from the log
 * details, a key that arrives over a question still waiting for its answer.
 */
@Injectable({ providedIn: 'root' })
class OpenDialogs {
  readonly list: OpenDialog[] = [];
}

/**
 * Lightweight centered modal dialog with a backdrop. Rendered with
 * position:fixed so it escapes any overflow-clipped ancestor. Content is
 * projected via <ng-content>. Emits `close` on backdrop click / close button /
 * Escape (unless `dismissable` is false).
 *
 * A dialog for the keyboard as well. When it comes up the focus moves into it: to its first
 * control — or the one its content marks `cdkFocusInitial` — and to the box itself when it
 * holds none. Tab and Shift+Tab go round inside it (the CDK's focus trap), and a focus that
 * lands outside while the dialog is up is brought back. When the dialog goes, the focus
 * returns to what had it before, if that is still in the page.
 */
@Component({
  selector: 'app-modal',
  imports: [LucideAngularModule],
  templateUrl: './modal.html',
  styleUrl: './modal.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
  host: { '(window:keydown.escape)': 'onEscape($event)' },
})
export class ModalComponent {
  readonly open = input<boolean>(false);
  readonly title = input<string>('');
  readonly dismissable = input(true, { transform: booleanAttribute });
  /** For a whole panel rather than a form (log details): up to 1100px wide, full height. */
  readonly wide = input(false, { transform: booleanAttribute });

  readonly close = output<void>();

  private readonly box = viewChild<ElementRef<HTMLElement>>('box');
  private readonly focusTraps = inject(FocusTrapFactory);
  private readonly injector = inject(Injector);
  private readonly dialogs = inject(OpenDialogs).list;

  constructor() {
    // After the render, not in it: by then the content is in the box, and whatever the same
    // change did to the page behind — a button disabled, another dialog taken away — is done.
    afterRenderEffect(onCleanup => {
      const box = this.box()?.nativeElement;
      if (!box) return;
      const dialog = this.comeUp(box);
      const stray = (focus: FocusEvent): void => this.keepFocus(dialog, focus);
      document.addEventListener('focus', stray, true);
      onCleanup(() => {
        document.removeEventListener('focus', stray, true);
        this.goAway(dialog);
      });
    });
  }

  onBackdrop(): void {
    if (this.dismissable()) this.close.emit();
  }

  /**
   * A press on the backdrop is no reason for the focus to leave the dialog: it would fall to
   * the page, and the next key would be the page's. A press inside the box is left alone —
   * that is how a text in it is selected.
   */
  onBackdropPress(press: MouseEvent): void {
    if (press.target === press.currentTarget) press.preventDefault();
  }

  /**
   * Escape puts the dialog away, as its Close button does. The key is read at the window, the
   * last place it comes to: every layer that can stand over the dialog or inside it — a menu, a
   * bottom sheet, the event detail, which closes its own layers one press at a time — listens
   * at the document or below, and one that took the key has marked it. So a press never closes
   * two layers, and of several dialogs only the one on top answers.
   */
  onEscape(key: Event): void {
    const box = this.box()?.nativeElement;
    if (key.defaultPrevented || !box || !this.dismissable()) return;
    if (this.dialogs.at(-1)?.box !== box) return;
    key.preventDefault();
    this.close.emit();
  }

  /** The dialog is on screen: it goes on top of the others and takes the focus. */
  private comeUp(box: HTMLElement): OpenDialog {
    const had = document.activeElement;
    // A control inside that has taken the focus already keeps it, and where the focus was
    // before that is not known.
    const inside = box.contains(had);
    const dialog: OpenDialog = {
      box,
      trap: this.focusTraps.create(box),
      returnTo: had instanceof HTMLElement && had !== document.body && !inside ? had : null,
    };
    this.dialogs.push(dialog);
    // Without scrolling: a dialog is read from its top, wherever its first control stands.
    if (!inside && !dialog.trap.focusInitialElement({ preventScroll: true })) box.focus({ preventScroll: true });
    return dialog;
  }

  /**
   * A focus that lands outside the dialog on top — Tab coming in from the browser's own
   * controls, the page focusing something of its own — is brought back: to the first control,
   * or to the box when it holds none, as it does while every button waits for an answer.
   * Looked at after the next render, not at once: the trap's own turn from the last control
   * to the first passes through an element outside the box, and a page that focuses something
   * of its own and closes the dialog in the same breath has closed it by then, and keeps its
   * focus. What the CDK's overlay holds — a menu opened from inside the dialog — is not
   * outside.
   */
  private keepFocus(dialog: OpenDialog, focus: FocusEvent): void {
    if (focus.target instanceof Node && dialog.box.contains(focus.target)) return;
    afterNextRender(() => {
      const now = document.activeElement;
      if (this.dialogs.at(-1) !== dialog || dialog.box.contains(now)) return;
      if (now?.closest('.cdk-overlay-container')) return;
      if (!dialog.trap.focusFirstTabbableElement()) dialog.box.focus();
    }, { injector: this.injector });
  }

  /** The dialog is gone: the focus returns to what had it, if that is still in the page. */
  private goAway(dialog: OpenDialog): void {
    const { box, returnTo } = dialog;
    dialog.trap.destroy();
    this.dialogs.splice(this.dialogs.indexOf(dialog), 1);
    // A dialog still up may have had its own place to return the focus to in this one — it
    // came up over it — or none at all, having come up in its place while the control that
    // asked for it waited, disabled: a key in place of the question about it. It returns the
    // focus where this one would have.
    for (const other of this.dialogs) {
      if (!other.returnTo?.isConnected) other.returnTo = returnTo;
    }
    // The focus is in such a dialog, or the page has put it somewhere on purpose while this
    // one was closing: it stays there.
    const now = document.activeElement;
    if (now && now !== document.body && !box.contains(now)) return;
    // A control that has left the page takes no focus, and the focus stays nowhere.
    returnTo?.focus();
  }
}
