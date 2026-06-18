import {
  Component, signal, computed, input, output,
  ChangeDetectionStrategy, HostListener,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';

type ViewMode = 'text' | 'json' | 'xml';

interface JsonToken {
  type: 'brace' | 'bracket' | 'key' | 'string' | 'number' | 'boolean' | 'null' | 'punctuation' | 'text';
  value: string;
}

/** Minimal syntax-highlight tokeniser for JSON → colored spans (no external deps). */
function tokenizeJson(json: string): JsonToken[] {
  const tokens: JsonToken[] = [];
  let i = 0;
  while (i < json.length) {
    const c = json[i];
    if (c === '{' || c === '}') { tokens.push({ type: 'brace', value: c }); i++; continue; }
    if (c === '[' || c === ']') { tokens.push({ type: 'bracket', value: c }); i++; continue; }
    if (c === ':' || c === ',') { tokens.push({ type: 'punctuation', value: c }); i++; continue; }
    if (c === '"') {
      let s = '"';
      i++;
      while (i < json.length) {
        if (json[i] === '\\') { s += json[i] + (json[i + 1] ?? ''); i += 2; continue; }
        s += json[i];
        if (json[i++] === '"') break;
      }
      // peek back: if next non-space is ':', it's a key
      let j = i;
      while (j < json.length && json[j] === ' ') j++;
      tokens.push({ type: json[j] === ':' ? 'key' : 'string', value: s });
      continue;
    }
    if (c === 't' || c === 'f' || c === 'n') {
      const kws = ['true', 'false', 'null'];
      const kw = kws.find(k => json.startsWith(k, i));
      if (kw) { tokens.push({ type: kw === 'null' ? 'null' : 'boolean', value: kw }); i += kw.length; continue; }
    }
    if (c === '-' || (c >= '0' && c <= '9')) {
      let num = '';
      while (i < json.length && (json[i] === '-' || json[i] === '+' || json[i] === '.' ||
             json[i] === 'e' || json[i] === 'E' || (json[i] >= '0' && json[i] <= '9'))) {
        num += json[i++];
      }
      tokens.push({ type: 'number', value: num });
      continue;
    }
    tokens.push({ type: 'text', value: c });
    i++;
  }
  return tokens;
}

function tokensToHtml(tokens: JsonToken[]): string {
  return tokens.map(t => {
    const v = t.value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    switch (t.type) {
      case 'key':       return `<span class="pv-key">${v}</span>`;
      case 'string':    return `<span class="pv-str">${v}</span>`;
      case 'number':    return `<span class="pv-num">${v}</span>`;
      case 'boolean':   return `<span class="pv-bool">${v}</span>`;
      case 'null':      return `<span class="pv-null">${v}</span>`;
      case 'brace':     return `<span class="pv-brace">${v}</span>`;
      case 'bracket':   return `<span class="pv-bracket">${v}</span>`;
      case 'punctuation': return `<span class="pv-punct">${v}</span>`;
      default:          return v;
    }
  }).join('');
}

/** Simple XML highlighter — wraps tags, attributes, text nodes. */
function highlightXml(xml: string): string {
  return xml
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    // restore tags after escaping
    .replace(/&lt;([!?/]?)([\w-:]+)((?:\s[\w-:="'&;]+)*)\s*(\/?&gt;)/g,
      (_, prefix, tag, attrs, close) => {
        const a = attrs.replace(/([\w-:]+)=/g, '<span class="pv-xml-attr">$1</span>=');
        return `<span class="pv-xml-punct">&lt;${prefix}</span><span class="pv-xml-tag">${tag}</span>${a}<span class="pv-xml-punct">${close}</span>`;
      })
    .replace(/&lt;(\/[\w-:]+)&gt;/g,
      '<span class="pv-xml-punct">&lt;</span><span class="pv-xml-tag">$1</span><span class="pv-xml-punct">&gt;</span>');
}

@Component({
  selector: 'app-prop-viewer',
  standalone: true,
  imports: [FormsModule, LucideAngularModule],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="pv-backdrop" (click)="close.emit()"></div>
    <div class="pv-panel" (click)="$event.stopPropagation()">
      <!-- Header -->
      <div class="pv-header">
        <span class="pv-title">{{ propKey() }}</span>
        <div class="pv-mode-tabs">
          <button [class.active]="mode() === 'text'" (click)="mode.set('text')">Text</button>
          <button [class.active]="mode() === 'json'" (click)="mode.set('json')">JSON</button>
          <button [class.active]="mode() === 'xml'"  (click)="mode.set('xml')">XML</button>
        </div>
        <button class="pv-close" (click)="close.emit()">
          <lucide-icon name="x" [size]="14" />
        </button>
      </div>

      <!-- Key filter (JSON/XML) -->
      @if (mode() !== 'text' && isStructured()) {
        <div class="pv-key-filter">
          <lucide-icon name="eye-off" [size]="12" />
          <span class="pv-kf-label">Hide keys:</span>
          <div class="pv-kf-tags">
            @for (k of allKeys(); track k) {
              <button
                class="pv-kf-tag"
                [class.hidden]="hiddenKeys().has(k)"
                (click)="toggleKey(k)">
                {{ k }}
              </button>
            }
          </div>
        </div>
      }

      <!-- Content -->
      <div class="pv-body">
        @if (mode() === 'text') {
          <pre class="pv-pre">{{ displayValue() }}</pre>
        } @else if (mode() === 'json') {
          <pre class="pv-pre pv-highlighted" [innerHTML]="highlightedHtml()"></pre>
        } @else if (mode() === 'xml') {
          <pre class="pv-pre pv-highlighted" [innerHTML]="highlightedHtml()"></pre>
        }
      </div>

      <!-- Footer -->
      <div class="pv-footer">
        <button class="pv-btn" (click)="copyValue()">
          <lucide-icon name="copy" [size]="12" /> Copy
        </button>
        <button class="pv-btn" (click)="copyFiltered()" [disabled]="hiddenKeys().size === 0">
          <lucide-icon name="filter" [size]="12" /> Copy filtered
        </button>
        <span class="pv-size">{{ byteSize() }} chars</span>
      </div>
    </div>
  `,
  styleUrl: './prop-viewer.scss',
})
export class PropViewerComponent {
  readonly propKey   = input.required<string>();
  readonly rawValue  = input.required<unknown>();
  readonly close     = output<void>();

  mode       = signal<ViewMode>('json');
  hiddenKeys = signal<Set<string>>(new Set());

  // Detect if value is JSON object/array or XML string
  isStructured = computed(() => {
    const v = this.rawValue();
    if (typeof v === 'object' && v !== null) return true;
    if (typeof v === 'string') {
      const s = (v as string).trimStart();
      if (s.startsWith('{') || s.startsWith('[')) {
        try { JSON.parse(s); return true; } catch { /* */ }
      }
      if (s.startsWith('<')) return true;
    }
    return false;
  });

  /** Parsed object — null when not parseable */
  private parsedObject = computed<unknown>(() => {
    const v = this.rawValue();
    if (typeof v === 'object') return v;
    if (typeof v === 'string') {
      try { return JSON.parse(v as string); } catch { return null; }
    }
    return null;
  });

  /** Flat list of all top-level keys (for the hide-keys toolbar) */
  allKeys = computed<string[]>(() => {
    const obj = this.parsedObject();
    if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
      return Object.keys(obj as Record<string, unknown>);
    }
    return [];
  });

  /** Value with hidden keys removed */
  private filteredObject = computed<unknown>(() => {
    const obj = this.parsedObject();
    const hidden = this.hiddenKeys();
    if (!obj || typeof obj !== 'object' || Array.isArray(obj) || hidden.size === 0) return obj ?? this.rawValue();
    const filtered: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(obj as Record<string, unknown>)) {
      if (!hidden.has(k)) filtered[k] = v;
    }
    return filtered;
  });

  displayValue = computed<string>(() => {
    const v = this.filteredObject();
    if (typeof v === 'string') return v;
    return JSON.stringify(v, null, 2);
  });

  highlightedHtml = computed<string>(() => {
    const mode = this.mode();
    const text = this.displayValue();
    if (mode === 'json') {
      try {
        const pretty = JSON.stringify(JSON.parse(text), null, 2);
        return tokensToHtml(tokenizeJson(pretty));
      } catch {
        return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      }
    }
    if (mode === 'xml') return highlightXml(text);
    return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  });

  byteSize = computed(() => this.displayValue().length);

  toggleKey(k: string): void {
    this.hiddenKeys.update(s => {
      const ns = new Set(s);
      if (ns.has(k)) ns.delete(k); else ns.add(k);
      return ns;
    });
  }

  async copyValue(): Promise<void> {
    await navigator.clipboard.writeText(this.displayValue());
  }

  async copyFiltered(): Promise<void> {
    await navigator.clipboard.writeText(this.displayValue());
  }

  @HostListener('document:keydown.escape')
  onEsc(): void { this.close.emit(); }
}
