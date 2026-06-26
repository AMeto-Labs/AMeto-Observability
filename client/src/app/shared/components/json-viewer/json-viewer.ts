import {
  Component, input, signal, computed, inject,
  ChangeDetectionStrategy,
} from '@angular/core';
import { LucideAngularModule } from 'lucide-angular';

import {
  JsonViewerActions, jvJoinKey, jvJoinIndex,
} from './json-viewer.actions';

interface Entry {
  key: string;
  value: unknown;
}

/** True for a value that should get its own expandable node (non-empty object/array). */
function isContainerValue(v: unknown): boolean {
  return v !== null && typeof v === 'object' && Object.keys(v as object).length > 0;
}

function escapeHtml(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

/**
 * Colorizes a single-line JSON string into class-tagged spans (matching the
 * `.jv-*` palette used by the expanded tree). Operates char-by-char so it
 * tolerates the trailing ` …` truncation marker — anything it doesn't recognize
 * is emitted as plain (escaped) text.
 */
function highlightJsonLine(json: string): string {
  let out = '';
  let i = 0;
  while (i < json.length) {
    const c = json[i];
    if (c === '{' || c === '}' || c === '[' || c === ']') {
      out += `<span class="jv-bracket">${c}</span>`; i++; continue;
    }
    if (c === ':' || c === ',') {
      out += `<span class="jv-colon">${c}</span>`; i++; continue;
    }
    if (c === '"') {
      let s = '"';
      i++;
      while (i < json.length) {
        if (json[i] === '\\') { s += json[i] + (json[i + 1] ?? ''); i += 2; continue; }
        s += json[i];
        if (json[i++] === '"') break;
      }
      // A string immediately followed by ':' is an object key.
      let j = i;
      while (j < json.length && json[j] === ' ') j++;
      out += `<span class="${json[j] === ':' ? 'jv-key' : 'jv-str'}">${escapeHtml(s)}</span>`;
      continue;
    }
    if (c === 't' || c === 'f' || c === 'n') {
      const kw = ['true', 'false', 'null'].find(k => json.startsWith(k, i));
      if (kw) {
        out += `<span class="${kw === 'null' ? 'jv-null' : 'jv-bool'}">${kw}</span>`;
        i += kw.length; continue;
      }
    }
    if (c === '-' || (c >= '0' && c <= '9')) {
      let num = '';
      while (i < json.length && /[-+.eE0-9]/.test(json[i])) num += json[i++];
      out += `<span class="jv-num">${num}</span>`;
      continue;
    }
    out += escapeHtml(c); i++;
  }
  return out;
}

/**
 * Seq/Datalust-style inline JSON viewer.
 *
 * Renders a structured value collapsed to a single line by default (`{ a: 1, … }`)
 * with a chevron toggle that expands it into an indented, recursively-collapsible
 * tree. Each nested object/array is itself an `<app-json-viewer>` (recursive,
 * self-referencing — no NgModule, no import of self required).
 */
@Component({
  selector: 'app-json-viewer',
  standalone: true,
  imports: [LucideAngularModule],
  changeDetection: ChangeDetectionStrategy.OnPush,
  host: { class: 'jv' },
  template: `
    @if (isContainer()) {
      <span class="jv-node" [class.jv-open]="expanded()">
        <span class="jv-head">
          <button
            type="button"
            class="jv-toggle"
            (click)="toggle($event)"
            [attr.aria-expanded]="expanded()"
            [title]="expanded() ? 'Collapse' : 'Expand'"
          >
            <lucide-icon [name]="expanded() ? 'chevron-down' : 'chevron-right'" [size]="10" />
          </button>

          @if (!expanded()) {
            <span class="jv-preview" (click)="toggle($event)" [innerHTML]="previewHtml()"></span>
          } @else {
            <span class="jv-bracket">{{ isArray() ? '[' : '{' }}</span>
          }

          @if (actions) {
            <button class="jv-menu-btn" type="button" title="Filter…"
                    (click)="openMenu($event, path(), value(), true)">
              <lucide-icon name="list-filter" [size]="11" />
            </button>
          }
        </span>

        @if (expanded()) {
          <span class="jv-children">
            @for (entry of entries(); track entry.key) {
              <span class="jv-row" [class.jv-row--hit]="rowMatches(entry)">
                @if (!isArray()) {
                  <span class="jv-key" [innerHTML]="hl(entry.key)"></span><span class="jv-colon">: </span>
                }
                @if (isContainerLeaf(entry.value)) {
                  <span [class]="leafClass(entry.value)" [innerHTML]="hl(leafText(entry.value))"></span>
                  @if (actions) {
                    <button class="jv-menu-btn" type="button" title="Filter…"
                            (click)="openMenu($event, childPath(entry), entry.value, false)">
                      <lucide-icon name="list-filter" [size]="11" />
                    </button>
                  }
                } @else {
                  <app-json-viewer [value]="entry.value" [path]="childPath(entry)" [searchTerm]="searchTerm()" />
                }
              </span>
            }
          </span>
          <span class="jv-bracket">{{ isArray() ? ']' : '}' }}</span>
        }
      </span>
    } @else {
      <span class="jv-leaf">
        <span [class]="leafClass(value())" [innerHTML]="hl(leafText(value()))"></span>
        @if (actions) {
          <button class="jv-menu-btn" type="button" title="Filter…"
                  (click)="openMenu($event, path(), value(), false)">
            <lucide-icon name="list-filter" [size]="11" />
          </button>
        }
      </span>
    }
  `,
  styleUrl: './json-viewer.scss',
})
export class JsonViewerComponent {
  readonly value = input.required<unknown>();
  /** Filter-language path of this value (e.g. "items" / "items[0].Id"); '' at root. */
  readonly path = input<string>('');
  /** Expand state on first render (top-level viewers default collapsed → one line). */
  readonly initialExpanded = input<boolean>(false);
  /** Search term — highlighted in keys and leaf values throughout the tree. */
  readonly searchTerm = input<string>('');

  /** Optional — present only when a host (e.g. event-row) provides the channel. */
  protected readonly actions = inject(JsonViewerActions, { optional: true });

  readonly expanded = signal(false);

  constructor() {
    // Apply the requested initial state once the input is available.
    queueMicrotask(() => this.expanded.set(this.initialExpanded()));
  }

  /** Full path of an entry, accounting for array-index vs object-key notation. */
  childPath(entry: Entry): string {
    return this.isArray()
      ? jvJoinIndex(this.path(), Number(entry.key))
      : jvJoinKey(this.path(), entry.key);
  }

  openMenu(e: MouseEvent, path: string, value: unknown, isContainer: boolean): void {
    e.stopPropagation();
    e.preventDefault();
    this.actions?.open({ path, rawValue: value, isContainer, x: e.clientX, y: e.clientY });
  }

  readonly isContainer = computed(() => isContainerValue(this.value()));
  readonly isArray = computed(() => Array.isArray(this.value()));

  readonly entries = computed<Entry[]>(() => {
    const v = this.value();
    if (Array.isArray(v)) return v.map((item, i) => ({ key: String(i), value: item }));
    if (v && typeof v === 'object') {
      return Object.entries(v as Record<string, unknown>).map(([key, value]) => ({ key, value }));
    }
    return [];
  });

  /** Single-line, length-capped JSON preview for the collapsed state. */
  readonly preview = computed(() => {
    let s: string;
    try { s = JSON.stringify(this.value()); } catch { s = String(this.value()); }
    return s.length > 140 ? s.slice(0, 140) + ' …' : s;
  });

  /** Colorized HTML for the collapsed preview (token classes match the expanded tree). */
  readonly previewHtml = computed(() => highlightJsonLine(this.preview()));

  toggle(e: Event): void {
    e.stopPropagation();
    this.expanded.update(x => !x);
  }

  /** A child rendered directly (not via a nested viewer) — i.e. a primitive/empty container. */
  isContainerLeaf(v: unknown): boolean {
    return !isContainerValue(v);
  }

  leafClass(v: unknown): string {
    if (v === null || v === undefined) return 'jv-null';
    if (typeof v === 'number') return 'jv-num';
    if (typeof v === 'boolean') return 'jv-bool';
    if (typeof v === 'object') return 'jv-empty';
    return 'jv-str';
  }

  /** Wraps all occurrences of searchTerm in the text with a highlight mark.
   *  Searches the raw (pre-escape) text to avoid false hits inside HTML entities. */
  hl(text: string): string {
    const q = this.searchTerm().trim();
    if (!q) return escapeHtml(text);
    const ql = q.toLowerCase();
    const tl = text.toLowerCase();
    let result = '';
    let i = 0;
    while (i < text.length) {
      const idx = tl.indexOf(ql, i);
      if (idx < 0) { result += escapeHtml(text.slice(i)); break; }
      result += escapeHtml(text.slice(i, idx))
        + `<mark class="jv-hit">${escapeHtml(text.slice(idx, idx + q.length))}</mark>`;
      i = idx + q.length;
    }
    return result;
  }

  /** True when a row's key or leaf value contains the search term. */
  rowMatches(entry: Entry): boolean {
    const q = this.searchTerm().trim().toLowerCase();
    if (!q) return false;
    if (entry.key.toLowerCase().includes(q)) return true;
    if (this.isContainerLeaf(entry.value)) {
      return this.leafText(entry.value).toLowerCase().includes(q);
    }
    return false;
  }

  leafText(v: unknown): string {
    if (v === null || v === undefined) return 'null';
    if (typeof v === 'string') return `"${v}"`;
    if (typeof v === 'object') return Array.isArray(v) ? '[]' : '{}';
    return String(v);
  }
}
