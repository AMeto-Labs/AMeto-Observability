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

describe('flame graph cut node', () => {
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

  it('says nothing when no node was cut', async () => {
    const fixture = await render(node(10, [node(4), node(5, [node(1)])]));
    const el: HTMLElement = fixture.nativeElement;
    expect(el.querySelectorAll('.fg-bar').length).toBe(4);
    expect(el.querySelector('.fg-bar--cut')).toBeNull();
    expect(el.querySelector('.fg-cut-note')).toBeNull();
    expect(el.querySelector('.fg-legend')?.textContent).not.toContain('Cut');
  });
});
