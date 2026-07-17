function escapeHtml(s: string): string {
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/**
 * Rendered-message colouring ("1b" design spec, flat variant).
 *
 * Colour is keyed off the property NAME — a stable hash, so one field is always one
 * colour and a long list scans down a column. Unlike stock 1b there is no chip
 * background: the property colour is applied as plain foreground text.
 *
 * The palette is pastel and deliberately outside the level hues (--lvl-*): the level
 * badge owns saturated colour, so a value can never read as a severity at a glance.
 */
const PROP_PALETTE = ['#6cd0dd', '#ef9fbb', '#cf9be6', '#5fcaca', '#8bb6de', '#d3bd93'] as const;

/** djb2-xor hash → stable palette slot for a property name. */
function propColor(name: string): string {
  const s = String(name ?? '');
  let h = 5381;
  for (let i = 0; i < s.length; i++) h = ((h * 33) ^ s.charCodeAt(i)) >>> 0;
  return PROP_PALETTE[h % PROP_PALETTE.length];
}

/** A scalar template parameter, coloured by its property's name — flat, no chip. */
function propSpan(text: string, name: string): string {
  return `<span class="msg-prop" style="--prop:${propColor(name)}">${escapeHtml(text)}</span>`;
}

/** A JSON key: property colour + italic. Only keys are highlighted inside JSON. */
function keySpan(text: string, name: string): string {
  return `<span class="msg-key" style="--prop:${propColor(name)}">${escapeHtml(text)}</span>`;
}

/**
 * Colourises an embedded JSON blob: structural punctuation recedes, each KEY gets the
 * colour hashed from its name (italic), and values are dimmed below the template text —
 * the key is the landmark the eye scans for, the value stays quiet until you look for
 * it. Null keeps its dim-italic marker.
 */
function renderJson(json: string): string {
  let out = '';
  let i = 0;
  const n = json.length;

  while (i < n) {
    const c = json[i];

    if (c === '{' || c === '}' || c === '[' || c === ']' || c === ':' || c === ',') {
      out += `<span class="msg-punct">${escapeHtml(c)}</span>`;
      i++;
      continue;
    }
    if (c === ' ') { out += ' '; i++; continue; }

    // Quoted string — a key when the next non-space char is ':', otherwise a value.
    if (c === '"') {
      let j = i + 1;
      let val = '';
      while (j < n && json[j] !== '"') {
        if (json[j] === '\\') { val += json[j] + (json[j + 1] ?? ''); j += 2; continue; }
        val += json[j];
        j++;
      }
      const raw = json.slice(i, j + 1);
      i = j + 1;

      let k = i;
      while (k < n && json[k] === ' ') k++;
      out += json[k] === ':' ? keySpan(raw, val) : `<span class="msg-val">${escapeHtml(raw)}</span>`;
      continue;
    }

    // Bare segment: number / bool / null / identifier — dimmed value except null.
    let j = i;
    while (j < n && '{}[]:,"'.indexOf(json[j]) < 0) j++;
    const seg = json.slice(i, j);
    i = j;

    if (seg.trim() === 'null') out += `<span class="msg-null">${escapeHtml(seg)}</span>`;
    else out += `<span class="msg-val">${escapeHtml(seg)}</span>`;
  }
  return out;
}

/** Renders one resolved template parameter, coloured by the property's name. */
function renderParam(val: unknown, key: string): string {
  if (val === null || val === undefined) return '<span class="msg-null">null</span>';
  // Structured values are expanded so each nested key gets its own colour.
  if (typeof val === 'object') return renderJson(JSON.stringify(val));
  return propSpan(String(val), key);
}

export function renderMessageHtml(
  template: string | null | undefined,
  props: Record<string, unknown> | null | undefined,
): string {
  const tmpl = template || '';
  if (!tmpl) return '';

  let result = '';
  const tokenRegex = /\{([^}]+)\}/g;
  let lastIndex = 0;
  let match: RegExpExecArray | null;

  tokenRegex.lastIndex = 0;
  while ((match = tokenRegex.exec(tmpl)) !== null) {
    result += escapeHtml(tmpl.slice(lastIndex, match.index));

    const token = match[1];
    // Strip Serilog destructuring/stringify prefixes (`@`, `$`) and Serilog
    // formatting hints (`Name:000`, `Name,10`) before looking up the property.
    let key = token.split(':')[0].split(',')[0].trim();
    if (key.startsWith('@') || key.startsWith('$')) key = key.slice(1);

    if (props && key in props) {
      result += renderParam(props[key], key);
    } else {
      result += `<span class="tmpl-token">${escapeHtml(match[0])}</span>`;
    }

    lastIndex = match.index + match[0].length;
  }

  result += escapeHtml(tmpl.slice(lastIndex));
  return result;
}

export function escapeHtmlExport(s: string): string {
  return escapeHtml(s);
}
