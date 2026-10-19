import { TestBed } from '@angular/core/testing';
import { provideZonelessChangeDetection } from '@angular/core';
import { ComponentFixture } from '@angular/core/testing';
import { LUCIDE_ICONS, LucideIconProvider, icons } from 'lucide-angular';
import { describe, it, expect, afterEach, vi } from 'vitest';

import { PropertyMenuComponent } from './property-menu';

/**
 * The menu is asked for at a point just below its ⋮ button and moves itself back inside the
 * window once it knows its size. Hosts used to clamp against a guessed height instead, and near
 * the bottom of the window the rest of a taller menu was simply cut off.
 */
describe('PropertyMenuComponent — stays inside the window', () => {
  let fixture: ComponentFixture<PropertyMenuComponent>;

  afterEach(() => {
    TestBed.resetTestingModule();
    vi.restoreAllMocks();
  });

  const render = async (x: number, y: number, floating = true) => {
    // jsdom lays nothing out, so the menu's size is given here: 240 × 300.
    vi.spyOn(HTMLElement.prototype, 'getBoundingClientRect').mockReturnValue(
      { width: 240, height: 300, x: 0, y: 0, top: 0, left: 0, right: 240, bottom: 300, toJSON: () => ({}) } as DOMRect);
    TestBed.configureTestingModule({
      providers: [
        provideZonelessChangeDetection(),
        { provide: LUCIDE_ICONS, multi: true, useValue: new LucideIconProvider(icons) },
      ],
    });
    fixture = TestBed.createComponent(PropertyMenuComponent);
    fixture.componentRef.setInput('x', x);
    fixture.componentRef.setInput('y', y);
    fixture.componentRef.setInput('propKey', 'OrderId');
    fixture.componentRef.setInput('floating', floating);
    await fixture.whenStable();
    fixture.detectChanges();
    await fixture.whenStable();
    return fixture.nativeElement.querySelector('.prop-menu') as HTMLElement;
  };

  it('stays where it was asked for when it fits', async () => {
    const menu = await render(100, 200);
    expect(menu.style.left).toBe('100px');
    expect(menu.style.top).toBe('200px');
  });

  it('moves up and left so none of it falls outside the window', async () => {
    const menu = await render(window.innerWidth - 50, window.innerHeight - 40);
    expect(menu.style.left).toBe(`${window.innerWidth - 240 - 8}px`);
    expect(menu.style.top).toBe(`${window.innerHeight - 300 - 8}px`);
  });

  it('is grouped under headings, and Copy condition says which operator', async () => {
    const menu = await render(100, 200);
    const sections = [...menu.querySelectorAll('.pm-section')].map(s => s.textContent?.trim());
    expect(sections).toEqual(['Copy', 'Find']);

    const copied: boolean[] = [];
    fixture.componentInstance.copyCondition.subscribe(neq => copied.push(neq));
    menu.querySelector<HTMLButtonElement>('[title="Copy OrderId = this value"]')!.click();
    menu.querySelector<HTMLButtonElement>('[title="Copy OrderId ≠ this value"]')!.click();
    expect(copied).toEqual([false, true]);
  });

  it('offers Any element only for a key inside an array', async () => {
    const menu = await render(100, 200);
    expect(menu.textContent).not.toContain('Any element');

    fixture.componentRef.setInput('wildcard', true);
    await fixture.whenStable();
    const asked: boolean[] = [];
    fixture.componentInstance.anyElement.subscribe(neq => asked.push(neq));
    const row = [...menu.querySelectorAll('.pm-row')].find(r => r.textContent?.includes('Any element'))!;
    row.querySelectorAll<HTMLButtonElement>('.pm-op').forEach(b => b.click());
    expect(asked).toEqual([false, true]);
  });

  it('inside an overlay it places nothing itself — the overlay does', async () => {
    const menu = await render(window.innerWidth - 50, window.innerHeight - 40, false);
    expect(menu.classList.contains('floating')).toBe(false);
    expect(menu.style.left).toBe('');
    expect(menu.style.top).toBe('');
  });
});
