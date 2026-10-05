import { TestBed } from '@angular/core/testing';
import { LUCIDE_ICONS, LucideIconProvider, icons } from 'lucide-angular';
import { of } from 'rxjs';
import {
  CUT_NOTE, FlamegraphComponent, FlamegraphNode, layoutFlamegraph, subtreeIds,
} from './flame-graph';
import { ApiService } from '../../../core/services/api.service';

/**
 * THE FLAME GRAPH AT DEPTH (#94, from the #99 review). The server now sends a tree of any depth up
 * to 4 096 levels and marks where it stopped (`truncated`). Two things followed on this side: the
 * layout walked the tree recursively — one stack frame per level, which V8 holds and no other
 * engine was checked for — and a cut node was drawn as an ordinary leaf, so a flame graph with
 * spans missing looked complete.
 */

// ── The layout as it was: recursive. The reference the iterative one must equal to the bit. ──

function recursiveLayout(r: FlamegraphNode): FlamegraphNode[] {
  const out: FlamegraphNode[] = [];
  const walk = (n: FlamegraphNode, depth: number, x: number, w: number) => {
    n._depth = depth;
    n._x = x;
    n._w = w;
    out.push(n);
    let cx = x;
    for (const c of n.children) {
      const cw = w * (c.totalMs / n.totalMs);
      walk(c, depth + 1, cx, cw);
      cx += cw;
    }
  };
  walk(r, 0, 0, 1);
  return out;
}

function recursiveIds(f: FlamegraphNode): Set<string> {
  const ids = new Set<string>();
  const collect = (n: FlamegraphNode) => { ids.add(n.spanId); n.children.forEach(collect); };
  collect(f);
  return ids;
}

// ── Trees ──

let nextId = 0;
function node(totalMs: number, children: FlamegraphNode[] = [], extra: Partial<FlamegraphNode> = {}): FlamegraphNode {
  return {
    spanId: (nextId++).toString(16).padStart(16, '0'), name: 'op', service: 'svc', kind: 'Internal',
    status: 'Ok', totalMs, selfMs: 0, children, ...extra,
  };
}

/** mulberry32: a seeded generator, so a failure names the tree it failed on. */
function rng(seed: number): () => number {
  return () => {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** `count` nodes, each hung under a random earlier one, with uneven durations (children overlap or leave gaps, as real spans do). */
function randomTree(seed: number, count: number): FlamegraphNode {
  const r = rng(seed);
  const all = [node(1000)];
  for (let i = 1; i < count; i++) {
    const parent = all[Math.floor(r() * all.length)];
    const child = node(r() * parent.totalMs * 0.9 + 0.001);
    parent.children.push(child);
    all.push(child);
  }
  return all[0];
}

/** A spine of `levels` levels, every tenth level with a sibling, the last level cut. */
function deepTree(levels: number): FlamegraphNode {
  let bottom = node(1, [], { truncated: true });
  for (let d = levels - 2; d >= 0; d--) {
    const kids = d % 10 === 0 ? [bottom, node(0.25)] : [bottom];
    bottom = node(bottom.totalMs + (d % 10 === 0 ? 0.5 : 0.01), kids);
  }
  return bottom;
}


/** A fresh copy of the tree — iterative: `structuredClone` itself overflows on 4 096 levels here. */
function cloneTree(root: FlamegraphNode): FlamegraphNode {
  const copy = (n: FlamegraphNode): FlamegraphNode => ({ ...n, children: [] });
  const top = copy(root);
  const stack: [FlamegraphNode, FlamegraphNode][] = [[root, top]];
  while (stack.length) {
    const [from, to] = stack.pop()!;
    for (const c of from.children) {
      const cc = copy(c);
      to.children.push(cc);
      stack.push([c, cc]);
    }
  }
  return top;
}
function shape(nodes: FlamegraphNode[]): unknown[] {
  return nodes.map(n => [n.spanId, n._depth, n._x, n._w]);
}

describe('flame graph layout', () => {
  it('is the recursive layout to the bit — order, depth, x and width — on a wide tree and a 4 096-level one', () => {
    for (const tree of [randomTree(7, 3_000), randomTree(8, 200), deepTree(4_096)]) {
      const expected = shape(recursiveLayout(cloneTree(tree)));
      const actual   = shape(layoutFlamegraph(cloneTree(tree)));
      expect(actual.length).toBe(expected.length);
      expect(actual).toEqual(expected);   // numbers compared with Object.is: exact
    }
  });

  it('collects the subtree the recursive collect did', () => {
    const tree = randomTree(9, 2_000);
    const flat = layoutFlamegraph(tree);
    for (const f of [tree, flat[1], flat[500], flat[flat.length - 1]])
      expect([...subtreeIds(f)].sort()).toEqual([...recursiveIds(f)].sort());
  });

  it('lays out a chain far deeper than an engine stack, where the recursive walk overflows', () => {
    const levels = 200_000;
    let bottom = node(1);
    for (let d = 1; d < levels; d++) bottom = node(1, [bottom]);

    expect(() => recursiveLayout(bottom)).toThrow(RangeError);   // the walk this replaced

    const flat = layoutFlamegraph(bottom);
    expect(flat.length).toBe(levels);
    expect(flat[levels - 1]._depth).toBe(levels - 1);
    expect(flat[levels - 1]._w).toBe(1);
    expect(subtreeIds(bottom).size).toBe(levels);
  });
});

async function render(tree: FlamegraphNode) {
  TestBed.configureTestingModule({
    imports: [FlamegraphComponent],
    providers: [
      { provide: ApiService, useValue: { getFlamegraph: () => of(tree) } },
      { provide: LUCIDE_ICONS, multi: true, useValue: new LucideIconProvider(icons) },
    ],
  });
  const fixture = TestBed.createComponent(FlamegraphComponent);
  fixture.componentRef.setInput('traceId', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
  fixture.detectChanges();
  await fixture.whenStable();
  fixture.detectChanges();
  return fixture;
}

describe('flame graph cut node', () => {
  it('is marked: its bar, its title, the hover tooltip, a toolbar note and the legend — and nothing else is', async () => {
    const cut  = node(6, [], { name: 'deepest-sent', truncated: true });
    const tree = node(10, [node(7, [cut]), node(2)]);
    const fixture = await render(tree);
    const el: HTMLElement = fixture.nativeElement;

    const bars = el.querySelectorAll<HTMLElement>('.fg-bar');
    expect(bars.length).toBe(4);
    const cutBars = el.querySelectorAll<HTMLElement>('.fg-bar--cut');
    expect(cutBars.length).toBe(1);
    expect(cutBars[0].title).toContain('deepest-sent');
    expect(cutBars[0].title).toContain(CUT_NOTE);
    for (const b of Array.from(bars)) if (b !== cutBars[0]) expect(b.title).not.toContain(CUT_NOTE);

    expect(el.querySelector('.fg-cut-note')?.textContent).toContain('1 branch cut');
    expect(el.querySelector('.fg-legend')?.textContent).toContain('Cut');

    cutBars[0].dispatchEvent(new MouseEvent('mouseenter'));
    fixture.detectChanges();
    expect(el.querySelector('.fg-tt-cut')?.textContent).toContain(CUT_NOTE);
  });

  it('counts the cut nodes in view: zoomed into a subtree without one, it says nothing is missing', async () => {
    const cut  = node(6, [], { name: 'deepest-sent', truncated: true });
    const tree = node(10, [node(7, [cut], { name: 'with-cut' }), node(2, [], { name: 'without-cut' })]);
    const fixture = await render(tree);
    const el: HTMLElement = fixture.nativeElement;
    const bar = (name: string) =>
      Array.from(el.querySelectorAll<HTMLElement>('.fg-bar')).find(b => b.title.startsWith(name + ' '))!;

    bar('without-cut').click();   // zoom into the subtree with no cut node
    fixture.detectChanges();
    expect(el.querySelectorAll('.fg-bar').length).toBe(1);
    expect(el.querySelector('.fg-cut-note')).toBeNull();
    expect(el.querySelector('.fg-legend')?.textContent).not.toContain('Cut');

    (el.querySelector('.fg-btn') as HTMLElement).click();   // reset
    fixture.detectChanges();
    bar('with-cut').click();      // and into the one that holds it
    fixture.detectChanges();
    expect(el.querySelectorAll('.fg-bar').length).toBe(2);
    expect(el.querySelector('.fg-cut-note')?.textContent).toContain('1 branch cut');
  });

  it('says nothing when no node was cut', async () => {
    const fixture = await render(node(10, [node(4), node(5, [node(1)])]));
    const el: HTMLElement = fixture.nativeElement;
    expect(el.querySelectorAll('.fg-bar').length).toBe(4);
    expect(el.querySelector('.fg-bar--cut')).toBeNull();
    expect(el.querySelector('.fg-cut-note')).toBeNull();
    expect(el.querySelector('.fg-legend')?.textContent).not.toContain('Cut');
  });
});

/**
 * A ZOOM INSIDE A ZOOMED VIEW (#94, from the #106 review). The zoomed view drew re-scaled COPIES of
 * the focused subtree's nodes, and a click on a bar focused whatever object the bar was drawn from
 * — inside a zoom, a copy, whose x, width and depth were already relative to the first zoom. Applied
 * to the full layout, they put the second zoom's bars at the wrong offset, or off the canvas.
 */
describe('flame graph zoom', () => {
  /**
   *   root 16 ┬ e 8
   *           └ a 8 ┬ d 4
   *                 └ b 4 ┬ c 2
   *                       └ f 1
   *
   * Durations in powers of two, so every fraction of the layout is exact. `cut` names the nodes
   * the server marked `truncated` (all of them leaves, as a cut node is sent).
   */
  function zoomTree(cut: readonly string[] = []): FlamegraphNode {
    const n = (name: string, totalMs: number, children: FlamegraphNode[] = []) =>
      node(totalMs, children, cut.includes(name) ? { name, truncated: true } : { name });
    return n('root', 16, [n('e', 8), n('a', 8, [n('d', 4), n('b', 4, [n('c', 2), n('f', 1)])])]);
  }

  /** The bar drawn for the node named `name`: its title starts with the name. */
  const bar = (el: HTMLElement, name: string) =>
    Array.from(el.querySelectorAll<HTMLElement>('.fg-bar')).find(b => b.title.startsWith(name + ' '))!;

  /** Every bar drawn, by node name: [left %, width %, top px]. */
  function geometry(el: HTMLElement): Record<string, [number, number, number]> {
    const out: Record<string, [number, number, number]> = {};
    for (const b of Array.from(el.querySelectorAll<HTMLElement>('.fg-bar'))) {
      const name = b.title.split(' ')[0];
      out[name] = [parseFloat(b.style.left), parseFloat(b.style.width), parseFloat(b.style.top)];
    }
    return out;
  }

  const WHOLE = {
    root: [0, 100, 0], e: [0, 50, 24], a: [50, 50, 24], d: [50, 25, 48], b: [75, 25, 48],
    c: [75, 12.5, 72], f: [87.5, 6.25, 72],
  };

  it('zooms into the node clicked inside a zoomed view: full width on the top row, its subtree at its own offsets', async () => {
    const fixture = await render(zoomTree());
    const el: HTMLElement = fixture.nativeElement;
    expect(geometry(el)).toEqual(WHOLE);

    bar(el, 'a').click();
    fixture.detectChanges();
    expect(geometry(el)).toEqual({
      a: [0, 100, 0], d: [0, 50, 24], b: [50, 50, 24], c: [50, 25, 48], f: [75, 12.5, 48],
    });

    bar(el, 'b').click();         // a bar of the zoomed view
    fixture.detectChanges();
    expect(geometry(el)).toEqual({ b: [0, 100, 0], c: [0, 50, 24], f: [50, 25, 24] });
    expect(el.querySelector<HTMLElement>('.fg-bar--focused')?.title).toMatch(/^b /);

    bar(el, 'f').click();         // and once more
    fixture.detectChanges();
    expect(geometry(el)).toEqual({ f: [0, 100, 0] });
  });

  it('shows the whole tree again on a second click on the focused bar, or on Reset zoom — from a zoom inside a zoom too', async () => {
    const fixture = await render(zoomTree());
    const el: HTMLElement = fixture.nativeElement;
    const zoomAThenB = () => {
      bar(el, 'a').click();
      fixture.detectChanges();
      bar(el, 'b').click();
      fixture.detectChanges();
      expect(Object.keys(geometry(el))).toEqual(['b', 'c', 'f']);
    };

    zoomAThenB();
    bar(el, 'b').click();         // the focused bar again
    fixture.detectChanges();
    expect(geometry(el)).toEqual(WHOLE);
    expect(el.querySelector('.fg-btn')).toBeNull();

    zoomAThenB();
    (el.querySelector('.fg-btn') as HTMLElement).click();   // Reset zoom
    fixture.detectChanges();
    expect(geometry(el)).toEqual(WHOLE);
    expect(el.querySelector('.fg-bar--focused')).toBeNull();
  });

  it('after a zoom inside a zoom, the tooltip and the cut note are about the bars in view', async () => {
    const fixture = await render(zoomTree(['e', 'd', 'f']));
    const el: HTMLElement = fixture.nativeElement;
    const note = () => el.querySelector('.fg-cut-note')?.textContent ?? '';
    expect(note()).toContain('3 branches cut');

    bar(el, 'a').click();
    fixture.detectChanges();
    expect(note()).toContain('2 branches cut');   // d and f; e is outside a

    bar(el, 'b').click();
    fixture.detectChanges();
    expect(note()).toContain('1 branch cut');     // f alone

    bar(el, 'f').dispatchEvent(new MouseEvent('mouseenter'));
    fixture.detectChanges();
    expect(el.querySelector('.fg-tooltip strong')?.textContent).toBe('f');
    expect(el.querySelector('.fg-tt-dur')?.textContent).toContain('1.00ms total');
    expect(el.querySelector('.fg-tt-cut')?.textContent).toContain(CUT_NOTE);

    bar(el, 'c').dispatchEvent(new MouseEvent('mouseenter'));
    fixture.detectChanges();
    expect(el.querySelector('.fg-tooltip strong')?.textContent).toBe('c');
    expect(el.querySelector('.fg-tt-cut')).toBeNull();
  });

  it('draws, after any chain of zooms, the clicked node\'s subtree as that subtree\'s own flame graph', async () => {
    const tree = randomTree(11, 400);
    const flat = layoutFlamegraph(tree);
    for (const n of flat) n.name = n.spanId;      // bars are found by name, and these ids are unique

    // The path from the root to the deepest node: one click per level, each inside the last zoom.
    const parentOf = new Map<FlamegraphNode, FlamegraphNode>();
    for (const n of flat) for (const c of n.children) parentOf.set(c, n);
    let deepest = tree;
    for (const n of flat) if (n._depth! > deepest._depth!) deepest = n;
    const path: FlamegraphNode[] = [];
    for (let n = deepest; n !== tree; n = parentOf.get(n)!) path.unshift(n);
    expect(path.length).toBeGreaterThan(4);

    const fixture = await render(tree);
    const el: HTMLElement = fixture.nativeElement;
    for (const focus of path) {
      bar(el, focus.name).click();
      fixture.detectChanges();
      const own = layoutFlamegraph(cloneTree(focus));   // the subtree laid out as a tree of its own
      const drawn = geometry(el);
      expect(Object.keys(drawn).sort()).toEqual(own.map(n => n.name).sort());
      for (const n of own) {
        const [left, width, top] = drawn[n.name];
        expect(left).toBeCloseTo(n._x! * 100, 4);
        expect(width).toBeCloseTo(Math.max(0.05, n._w! * 100), 4);
        expect(top).toBe(n._depth! * 24);
      }
    }
  });
});
