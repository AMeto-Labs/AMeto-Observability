function escapeHtml(s: string): string {
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function getValueClass(val: unknown): string {
  if (val === null || val === undefined) return 'msg-null';
  if (typeof val === 'number' || typeof val === 'boolean') return 'msg-num';
  if (typeof val === 'object') return 'msg-obj';
  return 'msg-val';
}

function formatValue(val: unknown): string {
  if (val === null || val === undefined) return 'null';
  if (typeof val === 'object') return JSON.stringify(val);
  return String(val);
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
      const val = props[key];
      const cls = getValueClass(val);
      result += `<span class="${cls}">${escapeHtml(formatValue(val))}</span>`;
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
