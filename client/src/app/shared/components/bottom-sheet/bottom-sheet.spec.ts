import { Component, provideZonelessChangeDetection, signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { InteractivityChecker } from '@angular/cdk/a11y';
import { describe, it, expect, afterEach } from 'vitest';

import { BottomSheetComponent } from './bottom-sheet';

/**
 * The sheet is a dialog in fact: on open the focus moves into it, on close it goes back to the
 * control that opened it, and Escape closes the sheet before any layer under it can take the key.
 * jsdom lays nothing out, so the CDK's focusability check — which reads geometry — is replaced by
 * one that reads the markup.
 */
@Component({
  imports: [BottomSheetComponent],
  template: `
    <button id="opener" type="button" (click)="open.set(true)">Filters</button>
    @if (open()) {
      <app-bottom-sheet label="Test sheet" (closed)="onClosed()">
        <button id="first" type="button">First</button>
        <button id="second" type="button">Second</button>
      </app-bottom-sheet>
    }
  `,
})
class HostComponent {
  readonly open = signal(false);
  closes = 0;
  onClosed(): void {
    this.closes++;
    this.open.set(false);
  }
}

const FOCUSABLE = 'button, [href], input, select, textarea, [tabindex]';
const byMarkup: Partial<InteractivityChecker> = {
  isDisabled: el => el.hasAttribute('disabled'),
  isVisible: () => true,
  isFocusable: el => el.matches(FOCUSABLE) && !el.hasAttribute('disabled'),
  isTabbable: el => el.matches(FOCUSABLE) && !el.hasAttribute('disabled') && el.getAttribute('tabindex') !== '-1',
};

describe('BottomSheetComponent', () => {
  afterEach(() => TestBed.resetTestingModule());

  const open = async () => {
    TestBed.configureTestingModule({
      providers: [provideZonelessChangeDetection(), { provide: InteractivityChecker, useValue: byMarkup }],
    });
    const fixture = TestBed.createComponent(HostComponent);
    await fixture.whenStable();
    const opener = fixture.nativeElement.querySelector('#opener') as HTMLButtonElement;
    opener.focus();
    opener.click();
    await fixture.whenStable();
    return { fixture, host: fixture.componentInstance, opener };
  };

  it('takes the focus into the panel when it opens and gives it back when it closes', async () => {
    const { fixture, host, opener } = await open();
    expect(document.activeElement?.id).toBe('first');
    expect(fixture.nativeElement.querySelector('.bs-panel')?.getAttribute('aria-modal')).toBe('true');

    host.open.set(false);
    await fixture.whenStable();
    expect(document.activeElement).toBe(opener);
  });

  it('closes on Escape, taking the key before the layers under it, and on the scrim', async () => {
    const { fixture, host } = await open();
    // A bubble-phase listener at the document, as the event detail's Escape is: it must find the
    // key taken, or it would close its drawer under the open sheet.
    const seen: boolean[] = [];
    const under = (e: KeyboardEvent): void => { if (e.key === 'Escape') seen.push(e.defaultPrevented); };
    document.addEventListener('keydown', under);
    const first = fixture.nativeElement.querySelector('#first') as HTMLElement;
    first.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true }));
    document.removeEventListener('keydown', under);
    expect(host.closes).toBe(1);
    expect(seen).toEqual([true]);
    await fixture.whenStable();
    expect(fixture.nativeElement.querySelector('.bs-panel')).toBeNull();

    // Gone with the sheet: a later Escape reaches nobody here.
    first.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true }));
    expect(host.closes).toBe(1);

    host.open.set(true);
    await fixture.whenStable();
    (fixture.nativeElement.querySelector('.bs-scrim') as HTMLElement).click();
    expect(host.closes).toBe(2);
  });
});
