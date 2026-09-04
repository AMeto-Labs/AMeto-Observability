import { TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';
import { LUCIDE_ICONS, LucideIconProvider, icons, AlertCircle } from 'lucide-angular';
import { Observable, Subscriber, NEVER, of } from 'rxjs';
import { measureElement as defaultMeasure } from '@tanstack/angular-virtual';
import { TracesComponent } from './traces';
import { ApiService } from '../../core/services/api.service';
import { TraceRowDto } from '../../core/models/span.model';
import { StreamFrame } from '../../core/models/stream.model';

/**
 * WHAT A ROW IS WORTH WHILE NOBODY CAN SEE IT.
 *
 * <p>The trace list is not unmounted when the user opens the Graph tab — `.main-content` is
 * hidden with `display: none`, deliberately, because `.trace-rows` is the scroll container and
 * unmounting it would return the user to scrollTop 0 every time they glance away. The cost of
 * that choice is that the measuring virtualizer stays mounted inside a subtree with no CSS box,
 * where every measurement is exactly 0.</p>
 *
 * <p>@tanstack/virtual has no guard: `resizeItem` writes whatever it is handed into
 * `itemSizeCache`, keyed here by traceId. And the writer is not only the component's own
 * afterRender pass — the library's ResizeObserver fires with a 0×0 border box the instant the
 * panel is hidden, for every row it is already observing. A row zeroed that way and then scrolled
 * out keeps its 0 until it is rendered again, while `getTotalSize()` goes on summing the cache,
 * so every row after it is positioned wrong.</p>
 *
 * <p>These tests drive the library's own callback body —
 * `resizeItem(index, options.measureElement(node, entry, instance))` — rather than a stand-in for
 * it, and the first assertion in each is the hazard: the DEFAULT measurement of the same hidden
 * element, which is 0.</p>
 */

/**
 * jsdom has no ResizeObserver, and here that absence is the opportunity: the virtualizer creates
 * its own and measures rows THROUGH it, so capturing the callback lets these tests fire the
 * library's real observer path — `resizeItem(index, options.measureElement(node, entry, this))`
 * against the real Virtualizer instance — rather than a stand-in for it. The instance the hook
 * receives is the raw one, which the Angular wrapper's signal proxy does not otherwise expose.
 */
interface CapturedObserver { cb: (entries: ResizeObserverEntry[], o: ResizeObserver) => void; nodes: Set<Element>; }
const observers: CapturedObserver[] = [];

class CapturingResizeObserver {
  private readonly rec: CapturedObserver;
  constructor(cb: (entries: ResizeObserverEntry[], o: ResizeObserver) => void) {
    this.rec = { cb, nodes: new Set<Element>() };
    observers.push(this.rec);
  }
  observe(el: Element): void { this.rec.nodes.add(el); }
  unobserve(el: Element): void { this.rec.nodes.delete(el); }
  disconnect(): void { this.rec.nodes.clear(); }
}
(globalThis as any).ResizeObserver = CapturingResizeObserver;
if (typeof window !== 'undefined') (window as any).ResizeObserver = CapturingResizeObserver;

// jsdom does no layout, so every offsetHeight is 0 — and a virtualizer told its viewport is 0px
// high renders nothing. Same shim as the other traces specs.
Object.defineProperty(HTMLElement.prototype, 'offsetHeight', {
  configurable: true,
  get(this: HTMLElement) { return this.classList.contains('trace-rows') ? 600 : 82; },
});

/**
 * The panel's `display`, as the DOM reports it. jsdom does no layout and returns an empty
 * DOMRectList for everything, which is by coincidence exactly what a `display: none` subtree
 * returns in a real browser — so this flag makes the distinction the test needs explicit rather
 * than relying on that coincidence.
 */
let panelIsDisplayed = true;
Element.prototype.getClientRects = function (): DOMRectList {
  return (panelIsDisplayed ? [{} as DOMRect] : []) as unknown as DOMRectList;
};

/** A ResizeObserver entry as the library reads it: border box only. */
function entryOf(blockSize: number): ResizeObserverEntry {
  return { borderBoxSize: [{ blockSize, inlineSize: 320 }] } as unknown as ResizeObserverEntry;
}

function row(id: string): TraceRowDto {
  return {
    traceId: id, spanId: 's' + id, name: 'GET /x', serviceName: 'api', services: ['api'],
    status: 'Ok', httpMethod: 'GET', httpPath: '/x', httpStatusCode: 200,
    startTimeUnixNano: 1_700_000_000_000_000_000, durationNanos: 1_000_000, spanCount: 1,
  };
}

describe('traces list — a hidden row is not a zero-height row', () => {
  let sub!: Subscriber<StreamFrame<TraceRowDto>>;

  async function boot(rows: TraceRowDto[]) {
    panelIsDisplayed = true;

    const api: Partial<ApiService> = {
      streamTraceList:  () => new Observable<StreamFrame<TraceRowDto>>(s => { sub = s; }),
      streamTraceQuery: () => NEVER as any,
      getTraceStats:    () => NEVER as any,
      getServiceNames:  () => of([]) as any,
      getSearchHistory: () => NEVER as any,
      recordSearch:     () => NEVER as any,
      getTrace:         () => NEVER as any,
      getTraceLogs:     () => NEVER as any,
    };

    TestBed.configureTestingModule({
      imports: [TracesComponent],
      providers: [
        provideRouter([]),
        { provide: ApiService, useValue: api },
        { provide: LUCIDE_ICONS, multi: true, useValue: new LucideIconProvider({ ...icons, AlertCircle }) },
      ],
    });

    const fixture = TestBed.createComponent(TracesComponent);
    fixture.detectChanges();
    await fixture.whenStable();

    for (const r of rows) sub.next({ kind: 'row', row: r });
    sub.complete();
    fixture.detectChanges();
    await fixture.whenStable();

    return fixture;
  }

  /**
   * Fires the virtualizer's OWN ResizeObserver for one row, the way the browser would: the
   * library then runs `resizeItem(index, options.measureElement(node, entry, this))` itself.
   */
  function observerFires(el: Element, blockSize: number): void {
    const o = observers.find(x => x.nodes.has(el));
    if (!o) throw new Error('the virtualizer is not observing this row');
    o.cb([{ ...entryOf(blockSize), target: el } as unknown as ResizeObserverEntry],
         undefined as unknown as ResizeObserver);
  }

  function renderedRows(fixture: any): Element[] {
    return Array.from((fixture.nativeElement as HTMLElement).querySelectorAll('[data-index]'));
  }

  it('keeps the measured height when the panel is hidden underneath it', async () => {
    const fixture = await boot(Array.from({ length: 60 }, (_, i) => row('t' + i)));
    const v = (fixture.componentInstance as any).rowVirtualizer;

    const rows = renderedRows(fixture);
    expect(rows.length).toBeGreaterThan(0);

    // On screen: rows measure 100px and the virtualizer takes it.
    for (const el of rows) observerFires(el, 100);
    expect(v.itemSizeCache.get('t0')).toBe(100);
    const totalOnScreen = v.getTotalSize();

    // The user opens the Graph tab. `.main-content` gets `display: none`, and the observer fires
    // once per observed row with a 0×0 border box.
    panelIsDisplayed = false;

    // THE HAZARD, stated as a measurement rather than a claim: the library's own default
    // measurement of this element, right now, is 0 — and `resizeItem` stores what it is handed.
    expect(defaultMeasure(rows[0] as any, entryOf(0), v)).toBe(0);

    for (const el of rows) observerFires(el, 0);

    expect(v.itemSizeCache.get('t0')).toBe(100);
    expect(v.getTotalSize()).toBe(totalOnScreen);
  });

  it('does not invent a height for a row it has never seen', async () => {
    // The other direction. Returning the ESTIMATE for an unmeasured row is not the same as
    // returning a stale measurement, and a hook that answered 0 here would be the original bug
    // wearing a guard; one that answered a measurement it never took would be worse.
    const fixture = await boot(Array.from({ length: 60 }, (_, i) => row('u' + i)));
    const v = (fixture.componentInstance as any).rowVirtualizer;
    const el = renderedRows(fixture)[0];

    // The component's own afterRender pass has already measured it at the jsdom shim's 82; drop
    // that so the row is genuinely unmeasured when the panel goes away.
    v.itemSizeCache.delete('u0');
    const before = v.getTotalSize();
    panelIsDisplayed = false;

    // Handing back the estimate is a NO-OP by construction: resizeItem compares against the
    // estimate already in measurementsCache, sees a zero delta and writes nothing. The row keeps
    // no measurement it never had, and the total does not move.
    expect(defaultMeasure(el as any, entryOf(0), v)).toBe(0);   // what would have been stored
    observerFires(el, 0);

    expect(v.itemSizeCache.get('u0')).toBeUndefined();
    expect(v.getTotalSize()).toBe(before);
  });

  it('takes the real height again as soon as the panel is shown', async () => {
    // The self-heal, which is the whole reason this belongs in `options.measureElement` and not
    // in a guard around the component's own afterRender pass: the rows stay observed while
    // hidden, so showing the panel fires the observer again with a real box and the truth lands
    // without anything having to notice the tab changed.
    const fixture = await boot(Array.from({ length: 60 }, (_, i) => row('v' + i)));
    const v = (fixture.componentInstance as any).rowVirtualizer;
    const el = renderedRows(fixture)[0];

    observerFires(el, 100);
    panelIsDisplayed = false;
    observerFires(el, 0);
    expect(v.itemSizeCache.get('v0')).toBe(100);

    panelIsDisplayed = true;
    observerFires(el, 137);                           // the row grew while nobody was looking
    expect(v.itemSizeCache.get('v0')).toBe(137);
  });
});
