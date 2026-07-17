import { ChangeDetectionStrategy, Component, input, output } from '@angular/core';

/**
 * Presentational dropdown for {@link SuggestInputDirective}: a flat, keyboard-navigable
 * list of completion candidates. The directive creates it dynamically, positions it
 * (fixed, under the input) and drives {@link items} / {@link activeIndex}; this component
 * only renders and reports pointer intent.
 *
 * `mousedown` is suppressed so clicking an item never blurs the input (which would close
 * the popup before the click lands) — the same trick the Events filter popup uses.
 */
@Component({
  selector: 'app-suggest-popup',
  changeDetection: ChangeDetectionStrategy.OnPush,
  host: {
    'class': 'suggest-pop',
    'role': 'listbox',
    '(mousedown)': '$event.preventDefault()',
  },
  template: `
    @for (item of items(); track item; let i = $index) {
      <div
        class="suggest-item"
        role="option"
        [class.active]="i === activeIndex()"
        (mouseenter)="hover.emit(i)"
        (click)="pick.emit(i)">{{ item }}</div>
    }
  `,
  styles: [`
    :host.suggest-pop {
      position: fixed;
      display: block;
      min-width: 200px;
      max-width: 460px;
      max-height: 260px;
      overflow-y: auto;
      background: var(--surface-up);
      border: 1px solid var(--border);
      border-radius: var(--radius-sm);
      box-shadow: var(--shadow);
      padding: 4px 0;
      z-index: 1000;
      font-size: 12px;
      font-family: var(--font-mono);
    }
    .suggest-item {
      padding: 3px 10px;
      color: var(--txt);
      cursor: pointer;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .suggest-item.active,
    .suggest-item:hover { background: var(--accent-bg); }
  `],
})
export class SuggestPopupComponent {
  readonly items = input<string[]>([]);
  readonly activeIndex = input<number>(0);
  readonly hover = output<number>();
  readonly pick = output<number>();
}
