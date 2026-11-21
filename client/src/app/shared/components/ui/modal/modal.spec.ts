import { Component, afterEveryRender, afterRenderEffect, provideZonelessChangeDetection, signal } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { InteractivityChecker } from '@angular/cdk/a11y';
import { LUCIDE_ICONS, LucideIconProvider, icons } from 'lucide-angular';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { ModalComponent } from './modal';

/**
 * The dialog and the keyboard: where the focus goes when a dialog comes up and when it goes,
 * that Tab cannot leave it, and what Escape does. jsdom lays nothing out and moves no focus on
 * Tab, so the CDK's focusability check — which reads geometry — is replaced by one that reads
 * the markup, and Tab is stood in for by the focus landing where Tab would put it.
 */
@Component({
  imports: [ModalComponent],
  template: `
    <button id="opener" type="button" (click)="open.set(true)">Open</button>
    <button id="elsewhere" type="button">Elsewhere</button>
    <app-modal [open]="open()" [title]="title()" [dismissable]="dismissable()" (close)="onClose()">
      @switch (content()) {
        @case ('buttons') {
          <button id="first" type="button">First</button>
          <button id="last" type="button">Last</button>
        }
        @case ('marked') {
          <button id="first" type="button">First</button>
          <button id="marked" type="button" cdkFocusInitial>Marked</button>
        }
        @case ('own') {
          <button id="first" type="button">First</button>
          <button id="own" type="button">Its own</button>
        }
        @case ('waiting') {
          <button id="first" type="button" disabled>First</button>
        }
      }
    </app-modal>
  `,
})
class OneDialogComponent {
  readonly open = signal(false);
  readonly title = signal('Question');
  readonly dismissable = signal(true);
  readonly content = signal<'buttons' | 'marked' | 'own' | 'waiting'>('buttons');
  /** False for a page that does not put the dialog away when asked to. */
  obeys = true;
  closes = 0;

  constructor() {
    // What the dialog shows takes the focus for itself, before the dialog has its turn.
    afterRenderEffect({
      earlyRead: () => {
        if (this.open() && this.content() === 'own') document.getElementById('own')?.focus();
      },
    });
  }

  onClose(): void {
    this.closes++;
    if (this.obeys) this.open.set(false);
  }
}

/** Three dialogs, named by where they stand in the page. */
@Component({
  imports: [ModalComponent],
  template: `
    <button id="opener" type="button">Open</button>
    <button id="elsewhere" type="button">Elsewhere</button>
    <app-modal id="one" [open]="one()" title="One" (close)="one.set(false)">
      <button id="in-one" type="button">In one</button>
    </app-modal>
    <app-modal id="two" [open]="two()" title="Two" [dismissable]="twoDismissable()" (close)="two.set(false)">
      <button id="in-two" type="button">In two</button>
    </app-modal>
    <app-modal id="three" [open]="three()" title="Three" (close)="three.set(false)">
      <button id="in-three" type="button">In three</button>
    </app-modal>
  `,
})
class DialogsComponent {
  readonly one = signal(false);
  readonly two = signal(false);
  readonly three = signal(false);
  readonly twoDismissable = signal(true);
}

const FOCUSABLE = 'button, [href], input, select, textarea, [tabindex]';
const byMarkup: Partial<InteractivityChecker> = {
  isDisabled: el => el.hasAttribute('disabled'),
  isVisible: () => true,
  isFocusable: el => el.matches(FOCUSABLE) && !el.hasAttribute('disabled'),
  isTabbable: el => el.matches(FOCUSABLE) && !el.hasAttribute('disabled') && el.getAttribute('tabindex') !== '-1',
};

describe('ModalComponent', () => {
  afterEach(() => {
    vi.restoreAllMocks();
    TestBed.resetTestingModule();
  });

  const create = async <T>(host: new () => T): Promise<ComponentFixture<T>> => {
    TestBed.configureTestingModule({
      providers: [
        provideZonelessChangeDetection(),
        { provide: InteractivityChecker, useValue: byMarkup },
        { provide: LUCIDE_ICONS, multi: true, useValue: new LucideIconProvider(icons) },
      ],
    });
    const fixture = TestBed.createComponent(host);
    await fixture.whenStable();
    return fixture;
  };

  const el = <E extends HTMLElement = HTMLElement>(f: ComponentFixture<unknown>, selector: string): E => {
    const found = (f.nativeElement as HTMLElement).querySelector<E>(selector);
    if (!found) throw new Error(`nothing matches ${selector}`);
    return found;
  };
  const all = (f: ComponentFixture<unknown>, selector: string): HTMLElement[] =>
    Array.from((f.nativeElement as HTMLElement).querySelectorAll<HTMLElement>(selector));

  /** What a stray focus is answered after: the dialog looks at it once the next render is done. */
  const rendered = (f: ComponentFixture<unknown>): Promise<unknown> => f.whenStable();

  /** A key pressed where the focus is — or on the page, when nothing has it. */
  const press = (key: string, init: KeyboardEventInit = {}): KeyboardEvent => {
    const event = new KeyboardEvent('keydown', { key, bubbles: true, cancelable: true, ...init });
    (document.activeElement ?? document.body).dispatchEvent(event);
    return event;
  };

  /** One dialog, opened from its button as a page opens it: the button has the focus. */
  const opened = async (set: (host: OneDialogComponent) => void = () => {}) => {
    const fixture = await create(OneDialogComponent);
    const host = fixture.componentInstance;
    set(host);
    const opener = el<HTMLButtonElement>(fixture, '#opener');
    opener.focus();
    opener.click();
    await fixture.whenStable();
    return { fixture, host, opener, box: el(fixture, '.modal') };
  };
  const closeButton = (f: ComponentFixture<unknown>, dialog = ''): HTMLElement =>
    el(f, `${dialog} .modal__header button[title="Close"]`);

  // ── Coming up ────────────────────────────────────────────────────────────────

  it('takes the focus to its first control when it comes up', async () => {
    // A dialog that can be put away has its Close button first, in the header.
    const dismissable = await opened();
    expect(document.activeElement).toBe(closeButton(dismissable.fixture));
    expect(dismissable.box.getAttribute('aria-modal')).toBe('true');
    TestBed.resetTestingModule();

    // One that cannot starts on the first control of what it shows.
    const held = await opened(host => host.dismissable.set(false));
    expect(all(held.fixture, '.modal button[title="Close"]')).toEqual([]);
    expect(document.activeElement).toBe(el(held.fixture, '#first'));
  });

  it('starts on the control its content marks, where it marks one', async () => {
    const { fixture } = await opened(host => host.content.set('marked'));
    expect(document.activeElement).toBe(el(fixture, '#marked'));
  });

  it('takes the focus to its own box when it holds no control', async () => {
    const { box } = await opened(host => {
      host.dismissable.set(false);
      host.content.set('waiting');
    });
    expect(document.activeElement).toBe(box);
    // Reachable by script only: with controls inside, the box is no stop for Tab.
    expect(box.getAttribute('tabindex')).toBe('-1');
  });

  it('is read from its top: taking the focus scrolls nothing', async () => {
    // A browser scrolls a focused control into view, and the first control may stand below the
    // fold of a dialog too tall for its window. jsdom scrolls nothing, so it is the request
    // that is held here.
    const focus = vi.spyOn(HTMLElement.prototype, 'focus');
    const asked = (target: HTMLElement): unknown[][] => focus.mock.calls.filter((_, i) => focus.mock.contexts[i] === target);

    const withControls = await opened();
    expect(asked(closeButton(withControls.fixture))).toEqual([[{ preventScroll: true }]]);
    TestBed.resetTestingModule();

    const without = await opened(host => {
      host.dismissable.set(false);
      host.content.set('waiting');
    });
    expect(asked(without.box)).toEqual([[{ preventScroll: true }]]);
  });

  it('leaves the focus with a control inside that has taken it for itself', async () => {
    const { fixture, host } = await opened(h => h.content.set('own'));
    expect(document.activeElement).toBe(el(fixture, '#own'));

    // Where the focus was before that control took it is not known: it is given back to nothing.
    host.open.set(false);
    await fixture.whenStable();
    expect(document.activeElement).toBe(document.body);
  });

  it('opens without a gesture as well: the focus comes from wherever it was', async () => {
    const fixture = await create(OneDialogComponent);
    const elsewhere = el(fixture, '#elsewhere');
    elsewhere.focus();
    fixture.componentInstance.open.set(true);
    await fixture.whenStable();
    expect(document.activeElement).toBe(closeButton(fixture));

    fixture.componentInstance.open.set(false);
    await fixture.whenStable();
    expect(document.activeElement).toBe(elsewhere);
  });

  it('leaves the focus alone when what it shows changes', async () => {
    const { fixture, host } = await opened();
    const last = el(fixture, '#last');
    last.focus();
    host.title.set('The next question');
    host.dismissable.set(false);
    await fixture.whenStable();
    expect(el(fixture, '.modal__title').textContent).toBe('The next question');
    expect(el(fixture, '#last')).toBe(last);
    expect(document.activeElement).toBe(last);
  });

  // ── Tab ──────────────────────────────────────────────────────────────────────

  it('keeps Tab and Shift+Tab inside: past the last control comes the first, before the first the last', async () => {
    const { fixture, box } = await opened();
    // The trap's two stops, one on each side of the box: Tab from the last control lands on the
    // one after it, Shift+Tab from the first on the one before.
    const [before, after] = [box.previousElementSibling as HTMLElement, box.nextElementSibling as HTMLElement];
    expect([before, after].map(stop => [stop.classList.contains('cdk-focus-trap-anchor'), stop.getAttribute('tabindex')]))
      .toEqual([[true, '0'], [true, '0']]);
    expect(el(fixture, '.modal-backdrop').children.length).toBe(3);

    el(fixture, '#last').focus();
    after.focus();
    expect(document.activeElement).toBe(closeButton(fixture));

    before.focus();
    expect(document.activeElement).toBe(el(fixture, '#last'));
    // And it stays there: the dialog's own look, after the render, finds the focus inside.
    await rendered(fixture);
    expect(document.activeElement).toBe(el(fixture, '#last'));
  });

  it('keeps Tab on the box of a dialog that holds no control', async () => {
    const { fixture, box } = await opened(host => {
      host.dismissable.set(false);
      host.content.set('waiting');
    });
    for (const stop of [box.nextElementSibling as HTMLElement, box.previousElementSibling as HTMLElement]) {
      stop.focus();
      expect(document.activeElement).toBe(stop);
      await rendered(fixture);
      expect(document.activeElement).toBe(box);
    }
  });

  // ── A focus that lands outside ───────────────────────────────────────────────

  it('brings back a focus that lands outside while it is up', async () => {
    const { fixture, host } = await opened();
    el(fixture, '#elsewhere').focus();
    expect(document.activeElement).toBe(el(fixture, '#elsewhere'));
    await rendered(fixture);
    expect(document.activeElement).toBe(closeButton(fixture));

    // With every control waiting for an answer there is only the box to bring it back to.
    host.dismissable.set(false);
    host.content.set('waiting');
    await fixture.whenStable();
    el(fixture, '#elsewhere').focus();
    await rendered(fixture);
    expect(document.activeElement).toBe(el(fixture, '.modal'));
  });

  it('leaves a focus that went into an overlay: a menu opened from inside the dialog', async () => {
    const { fixture } = await opened();
    const overlay = document.createElement('div');
    overlay.className = 'cdk-overlay-container';
    overlay.innerHTML = '<div class="cdk-overlay-pane"><button type="button">Copy value</button></div>';
    document.body.appendChild(overlay);
    try {
      const item = overlay.querySelector('button')!;
      item.focus();
      await rendered(fixture);
      expect(document.activeElement).toBe(item);
    } finally {
      overlay.remove();
    }
    expect(fixture.componentInstance.closes).toBe(0);
  });

  it('asks nothing of the page while the focus moves inside it', async () => {
    const { fixture } = await opened();
    let renders = 0;
    const counting = TestBed.runInInjectionContext(() => afterEveryRender(() => { renders++; }));
    await fixture.whenStable();
    renders = 0;

    // From one control to the next, as Tab goes: nothing to look at, and no render asked for.
    el(fixture, '#first').focus();
    el(fixture, '#last').focus();
    await rendered(fixture);
    expect(renders).toBe(0);

    // A focus outside is looked at after a render of its own.
    el(fixture, '#elsewhere').focus();
    await rendered(fixture);
    expect(renders).toBe(1);
    counting.destroy();
  });

  it('holds no focus once it is gone, and no longer listens for one', async () => {
    const { fixture, host } = await opened();
    host.open.set(false);
    await fixture.whenStable();
    let renders = 0;
    const counting = TestBed.runInInjectionContext(() => afterEveryRender(() => { renders++; }));
    await fixture.whenStable();
    renders = 0;

    el(fixture, '#elsewhere').focus();
    await rendered(fixture);
    expect(document.activeElement).toBe(el(fixture, '#elsewhere'));
    expect(renders).toBe(0);
    counting.destroy();
  });

  // ── Going away ───────────────────────────────────────────────────────────────

  it('gives the focus back to what had it when it goes', async () => {
    const { fixture, host, opener } = await opened();
    el(fixture, '#last').focus();
    host.open.set(false);
    await fixture.whenStable();
    expect(all(fixture, '.modal')).toEqual([]);
    expect(document.activeElement).toBe(opener);
  });

  it('gives the focus back to nothing when what had it has left the page', async () => {
    const { fixture, host, opener } = await opened();
    opener.remove();
    host.open.set(false);
    await fixture.whenStable();
    expect(all(fixture, '.modal')).toEqual([]);
    expect(document.activeElement).toBe(document.body);
  });

  it('leaves the focus where the page has put it while closing the dialog', async () => {
    // Closed first and focused after, and the other way round: either way the page's choice stands.
    for (const closeFirst of [true, false]) {
      const { fixture, host } = await opened();
      const elsewhere = el(fixture, '#elsewhere');
      if (closeFirst) host.open.set(false);
      elsewhere.focus();
      if (!closeFirst) host.open.set(false);
      await fixture.whenStable();
      expect(all(fixture, '.modal')).toEqual([]);
      expect(document.activeElement).toBe(elsewhere);
      TestBed.resetTestingModule();
    }
  });

  it('gives the focus back when it is destroyed while up', async () => {
    const { fixture, opener } = await opened();
    // Kept in the page, as a control outside the part that goes away is.
    document.body.appendChild(opener);
    try {
      fixture.destroy();
      expect(document.activeElement).toBe(opener);
    } finally {
      opener.remove();
    }
  });

  // ── Escape ───────────────────────────────────────────────────────────────────

  it('is put away by Escape when it can be put away', async () => {
    const { fixture, host, opener } = await opened();
    const key = press('Escape');
    expect(host.closes).toBe(1);
    // Taken: nothing that hears the key afterwards closes a second layer on it.
    expect(key.defaultPrevented).toBe(true);
    await fixture.whenStable();
    expect(all(fixture, '.modal')).toEqual([]);
    expect(document.activeElement).toBe(opener);

    // Gone: a later Escape is nobody's here.
    expect(press('Escape').defaultPrevented).toBe(false);
    expect(host.closes).toBe(1);
  });

  it('answers Escape wherever the focus is, and no other key', async () => {
    const { fixture, host } = await opened(h => { h.obeys = false; });
    (document.activeElement as HTMLElement).blur();
    expect(document.activeElement).toBe(document.body);
    press('Escape');
    expect(host.closes).toBe(1);

    el(fixture, '#first').focus();
    for (const key of ['Enter', ' ', 'Tab', 'e']) press(key);
    // Escape with a modifier is another key: the browser's, or the system's.
    press('Escape', { shiftKey: true });
    expect(host.closes).toBe(1);
  });

  it('is not put away by Escape when it cannot be put away', async () => {
    const { fixture, host } = await opened(h => h.dismissable.set(false));
    const key = press('Escape');
    expect(host.closes).toBe(0);
    expect(key.defaultPrevented).toBe(false);
    await fixture.whenStable();
    expect(all(fixture, '.modal').length).toBe(1);
    expect(document.activeElement).toBe(el(fixture, '#first'));

    // Whether it can is asked when the key comes, not when the dialog came up.
    host.dismissable.set(true);
    await fixture.whenStable();
    press('Escape');
    expect(host.closes).toBe(1);
  });

  it('leaves Escape to a layer that has taken it', async () => {
    const { host } = await opened();
    // As the event detail does for a menu open inside the log details: at the document, taken.
    const layer = (e: KeyboardEvent): void => { if (e.key === 'Escape') e.preventDefault(); };
    document.addEventListener('keydown', layer);
    try {
      press('Escape');
    } finally {
      document.removeEventListener('keydown', layer);
    }
    expect(host.closes).toBe(0);
  });

  // ── The backdrop ─────────────────────────────────────────────────────────────

  it('keeps the focus when the backdrop is pressed, and lets a press inside the box be', async () => {
    const { fixture, host } = await opened(h => h.dismissable.set(false));
    const pressOn = (target: HTMLElement): boolean => {
      const down = new MouseEvent('mousedown', { bubbles: true, cancelable: true });
      target.dispatchEvent(down);
      return down.defaultPrevented;
    };
    // The browser's answer to a press is to move the focus to what was pressed — for the
    // backdrop, to nothing, and so to the page.
    expect(pressOn(el(fixture, '.modal-backdrop'))).toBe(true);
    expect(pressOn(el(fixture, '.modal__title'))).toBe(false);
    expect(pressOn(el(fixture, '#first'))).toBe(false);

    el(fixture, '.modal-backdrop').click();
    expect(host.closes).toBe(0);
    host.dismissable.set(true);
    await fixture.whenStable();
    el(fixture, '.modal-backdrop').click();
    expect(host.closes).toBe(1);
  });

  // ── More than one dialog ─────────────────────────────────────────────────────

  const several = async () => {
    const fixture = await create(DialogsComponent);
    const opener = el(fixture, '#opener');
    opener.focus();
    return { fixture, host: fixture.componentInstance, opener };
  };
  const titles = (f: ComponentFixture<unknown>): (string | null)[] => all(f, '.modal__title').map(t => t.textContent);

  it('stands over another: the one that came up last has the focus and answers Escape', async () => {
    const { fixture, host, opener } = await several();
    host.one.set(true);
    await fixture.whenStable();
    el(fixture, '#in-one').focus();
    host.two.set(true);
    await fixture.whenStable();
    expect(document.activeElement).toBe(closeButton(fixture, '#two'));

    // A stray focus is the top one's to bring back, not a tug between the two: the one under
    // it does not take the focus on its way.
    const underIt: EventTarget[] = [];
    const seen = (e: FocusEvent): void => { underIt.push(e.target!); };
    el(fixture, '#one .modal').addEventListener('focusin', seen);
    el(fixture, '#elsewhere').focus();
    await rendered(fixture);
    expect(document.activeElement).toBe(closeButton(fixture, '#two'));
    expect(underIt).toEqual([]);
    el(fixture, '#one .modal').removeEventListener('focusin', seen);
    el(fixture, '#in-one').focus();
    await rendered(fixture);
    expect(document.activeElement).toBe(closeButton(fixture, '#two'));

    // One press, one dialog — and the focus goes back into the one under it, where it was.
    press('Escape');
    await fixture.whenStable();
    expect(titles(fixture)).toEqual(['One']);
    expect(document.activeElement).toBe(el(fixture, '#in-one'));

    press('Escape');
    await fixture.whenStable();
    expect(titles(fixture)).toEqual([]);
    expect(document.activeElement).toBe(opener);
  });

  it('does not let Escape through to the dialog under one that cannot be put away', async () => {
    const { fixture, host } = await several();
    host.one.set(true);
    await fixture.whenStable();
    host.twoDismissable.set(false);
    host.two.set(true);
    await fixture.whenStable();
    expect(press('Escape').defaultPrevented).toBe(false);
    await fixture.whenStable();
    expect(titles(fixture)).toEqual(['One', 'Two']);
    expect(document.activeElement).toBe(el(fixture, '#in-two'));
  });

  it('keeps the focus in the dialog on top when the one under it goes', async () => {
    const { fixture, host, opener } = await several();
    host.one.set(true);
    await fixture.whenStable();
    host.two.set(true);
    await fixture.whenStable();
    const inTwo = el(fixture, '#in-two');
    inTwo.focus();

    // The question under it is answered and goes: the focus is not pulled out to the page.
    host.one.set(false);
    await fixture.whenStable();
    expect(titles(fixture)).toEqual(['Two']);
    expect(document.activeElement).toBe(inTwo);

    // What the top one would have given the focus back to went with the other; it gives it
    // back where that one would have.
    host.two.set(false);
    await fixture.whenStable();
    expect(document.activeElement).toBe(opener);
  });

  it('takes the place of another in one change, and gives the focus back where the first would have', async () => {
    // Both ways round: whichever of the two stands first in the page, and so hears of the
    // change first.
    for (const [first, second] of [['one', 'two'], ['two', 'one']] as const) {
      const { fixture, host, opener } = await several();
      host[first].set(true);
      await fixture.whenStable();
      expect(document.activeElement).toBe(closeButton(fixture, `#${first}`));

      host[first].set(false);
      host[second].set(true);
      await fixture.whenStable();
      expect(titles(fixture)).toEqual([second === 'one' ? 'One' : 'Two']);
      expect(document.activeElement).toBe(closeButton(fixture, `#${second}`));

      host[second].set(false);
      await fixture.whenStable();
      expect(document.activeElement).toBe(opener);
      TestBed.resetTestingModule();
    }
  });

  it('gives the focus back through the dialogs under it, each to where it came from', async () => {
    const { fixture, host, opener } = await several();
    host.one.set(true);
    await fixture.whenStable();
    el(fixture, '#in-one').focus();
    host.two.set(true);
    await fixture.whenStable();
    el(fixture, '#in-two').focus();
    host.three.set(true);
    await fixture.whenStable();
    expect(document.activeElement).toBe(closeButton(fixture, '#three'));

    // The lowest goes first. The one on top came from the middle one, which is still there:
    // that is where it gives the focus back.
    host.one.set(false);
    await fixture.whenStable();
    expect(document.activeElement).toBe(closeButton(fixture, '#three'));
    host.three.set(false);
    await fixture.whenStable();
    expect(document.activeElement).toBe(el(fixture, '#in-two'));

    // The middle one came from the lowest, which is gone: it gives the focus back where that
    // one would have.
    host.two.set(false);
    await fixture.whenStable();
    expect(document.activeElement).toBe(opener);
  });
});
