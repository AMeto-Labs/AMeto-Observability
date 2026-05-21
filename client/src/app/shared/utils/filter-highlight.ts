/**
 * Syntax-highlights a Seq Filter Expression string into an HTML string.
 *
 * Token priority (first match wins, left-to-right in regex alternation):
 *   1. String literals  'value'
 *   2. Keywords         and or not like in is null true false
 *   3. Operators        = <> != <= >= < > ^=
 *   4. Numbers          123  3.14
 *   5. Punctuation      ( ) [ ] ,
 *   6. Identifiers      @l  RequestPath  UserId  etc.
 */

function esc(s: string): string {
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

const TOKEN_RE = new RegExp(
  [
    /('[^']*')/.source,                             // fh-str
    /(\b(?:and|or|not|like|in|is|null|true|false)\b)/.source, // fh-kw  (case-insensitive via flag)
    /(<>|!=|<=|>=|\^=|[=<>])/.source,              // fh-op
    /(\b\d+(?:\.\d+)?\b)/.source,                  // fh-num
    /([()[\],])/.source,                            // fh-paren
    /(@?[A-Za-z_][A-Za-z0-9_.]*)/.source,          // fh-ident
  ].join('|'),
  'gi',
);

export function highlightFilterExpression(expr: string): string {
  if (!expr) return '';

  TOKEN_RE.lastIndex = 0;
  let result = '';
  let last = 0;
  let m: RegExpExecArray | null;

  while ((m = TOKEN_RE.exec(expr)) !== null) {
    if (m.index > last) result += esc(expr.slice(last, m.index));

    const [full, str, kw, op, num, paren] = m;
    if      (str)   result += `<span class="fh-str">${esc(full)}</span>`;
    else if (kw)    result += `<span class="fh-kw">${esc(full)}</span>`;
    else if (op)    result += `<span class="fh-op">${esc(full)}</span>`;
    else if (num)   result += `<span class="fh-num">${esc(full)}</span>`;
    else if (paren) result += `<span class="fh-paren">${esc(full)}</span>`;
    else            result += `<span class="fh-ident">${esc(full)}</span>`;

    last = m.index + full.length;
  }

  if (last < expr.length) result += esc(expr.slice(last));
  return result;
}
