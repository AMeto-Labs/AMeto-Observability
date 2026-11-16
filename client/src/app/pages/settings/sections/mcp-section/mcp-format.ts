import { McpInputSchema, McpMaskedBy } from '../../../../core/models/mcp.model';

/**
 * How Settings → AI / MCP writes things down: numbers, windows, times, tokens, placeholders,
 * and the snippets a client is connected with. Pure functions — the tabs call them from
 * computeds, and the spec next to this file pins what each one prints.
 */

// ── Numbers and time ──────────────────────────────────────────────────────────

/** `48310` → `48 310`, with a no-break space so a figure never wraps. A missing value is a dash. */
export function fmtCount(n: number | null | undefined): string {
  if (n === null || n === undefined || !Number.isFinite(n)) return '—';
  return Math.round(n).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
}

/**
 * A window in minutes as a person writes it: `15 min`, `90 min`, `24 h`, `7 d`. A single day is
 * said in hours, and a window that is no whole number of hours in minutes: one number and one
 * unit, always. A form is filled with this and saved through {@link parseWindow}, so whatever
 * is printed here has to read back there as the same number.
 */
export function fmtWindow(minutes: number): string {
  if (!Number.isFinite(minutes) || minutes <= 0) return '—';
  if (minutes % 1440 === 0 && minutes >= 2880) return `${minutes / 1440} d`;
  if (minutes % 60 === 0) return `${minutes / 60} h`;
  return `${minutes} min`;
}

/** Minutes in a unit of a window, by the unit's first letter. */
const WINDOW_UNITS: Readonly<Record<string, number>> = { w: 10_080, d: 1_440, h: 60, m: 1 };

/**
 * The same, read back: `15m`, `90 min`, `24h`, `7 d`, `1w` → minutes — and the compound form a
 * person also writes, `1 h 30 min` or `1h30m`: units from the largest down, each at most once.
 * A bare number is refused rather than guessed at — `48` after `24 h` means hours to the person
 * typing and minutes to the API.
 */
export function parseWindow(text: string): number | null {
  const source = text ?? '';
  // Sticky: each part has to start exactly where the one before it ended.
  const part = /\s*(\d+(?:[.,]\d+)?)\s*(weeks?|w|days?|d|hours?|h|minutes?|mins?|m)(?![a-z])\s*/iy;
  let minutes = 0;
  let smallest = Infinity;
  let at = 0;
  while (at < source.length) {
    part.lastIndex = at;
    const m = part.exec(source);
    if (!m) return null;
    const unit = WINDOW_UNITS[m[2][0].toLowerCase()];
    if (unit >= smallest) return null;
    smallest = unit;
    minutes += parseFloat(m[1].replace(',', '.')) * unit;
    at = part.lastIndex;
  }
  return Number.isFinite(minutes) && minutes >= 1 ? Math.round(minutes) : null;
}

/**
 * What a number field holds, as a count: a whole number, or null while the field is empty or
 * holds no number. Empty is not turned into zero — a field cleared to be retyped would show a
 * `0` nobody typed. Whether the count will do is asked when the form is saved ({@link isCount}).
 */
export function asCount(value: unknown): number | null {
  if (value === null || value === undefined || value === '') return null;
  const n = Math.floor(Number(value));
  return Number.isFinite(n) ? n : null;
}

/** True for a count a limit can be: a whole number above zero. */
export function isCount(value: number | null | undefined): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value > 0;
}

const pad = (n: number): string => String(n).padStart(2, '0');

/** `09:14:31`, in the browser's zone. */
export function clockTime(iso: string): string {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? '' : `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

/** `2026-11-10`, in the browser's zone. */
export function dayOf(iso: string): string {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? '' : `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

/**
 * When a profile last called, the way a list wants it: the gap while it is short, the clock
 * for earlier today, then `yesterday`, then the date.
 */
export function lastCall(iso: string | null, now: number = Date.now()): string {
  if (!iso) return 'never';
  const at = new Date(iso);
  const t = at.getTime();
  if (Number.isNaN(t)) return 'never';
  const gap = now - t;
  if (gap < 60_000) return 'just now';
  if (gap < 3_600_000) return `${Math.floor(gap / 60_000)} min ago`;
  if (gap < 6 * 3_600_000) return `${Math.floor(gap / 3_600_000)} h ago`;
  const today = new Date(now);
  if (at.toDateString() === today.toDateString()) return `${pad(at.getHours())}:${pad(at.getMinutes())}`;
  if (at.toDateString() === new Date(now - 86_400_000).toDateString()) return 'yesterday';
  return dayOf(iso);
}

// ── Tools ─────────────────────────────────────────────────────────────────────

/** The three arguments every published tool takes besides its own. */
const COMMON_ARGUMENTS: ReadonlySet<string> = new Set(['from', 'to', 'cursor']);

/**
 * A tool's arguments in one line — `merchantId, reason?` — an optional one with a question
 * mark. `ownOnly` leaves out the window and the cursor, which every published tool has.
 */
export function signatureOf(schema: McpInputSchema | null | undefined, ownOnly = false, separator = ', '): string {
  const required = new Set(schema?.required ?? []);
  return Object.keys(schema?.properties ?? {})
    .filter(name => !ownOnly || !COMMON_ARGUMENTS.has(name))
    .map(name => (required.has(name) ? name : name + '?'))
    .join(separator);
}

/** What a call's status is to the eye: fine, cut short, or not answered. */
export function statusTone(status: string | null | undefined): 'ok' | 'warn' | 'err' | '' {
  switch (status) {
    case 'OK': return 'ok';
    case 'Trimmed': return 'warn';
    case 'Refused':
    case 'Timed out':
    case 'Failed': return 'err';
    default: return '';
  }
}

/** A refusal's kind in the words of a sentence: "refused by …". */
export const REFUSAL_KINDS: Readonly<Record<string, string>> = {
  input: 'the arguments',
  limit: 'a limit',
  masking: 'the masking rules',
  scope: 'the profile’s scope',
  unavailable: 'the log store',
};

/** JSON as sent, laid out for reading; text that is not JSON is shown as it is. */
export function prettyJson(text: string | null | undefined): string {
  if (!text) return '';
  try { return JSON.stringify(JSON.parse(text), null, 2); } catch { return text; }
}

// ── Highlighting ──────────────────────────────────────────────────────────────

/** A run of text and what it is: plain, a token, a `{parameter}`, or a stored value an answer hides. */
export interface Segment {
  text: string;
  kind: '' | 'token' | 'param' | 'raw';
}

const TOKEN = /h:[0-9a-f]{12}/g;

/** Splits a text at its masking tokens (`h:` and twelve hex digits). */
export function tokenSegments(text: string | null | undefined): Segment[] {
  const out: Segment[] = [];
  if (!text) return out;
  let last = 0;
  for (const m of text.matchAll(TOKEN)) {
    if (m.index > last) out.push({ text: text.slice(last, m.index), kind: '' });
    out.push({ text: m[0], kind: 'token' });
    last = m.index + m[0].length;
  }
  if (last < text.length) out.push({ text: text.slice(last), kind: '' });
  return out;
}

/** True when a text carries at least one masking token. */
export function hasToken(text: string | null | undefined): boolean {
  return !!text && text.search(/h:[0-9a-f]{12}/) >= 0;
}

/**
 * A placeholder's name, by the server's rule (`FilterTemplate.IsIdentifier`): a letter, then
 * letters, digits or underscores — 32 characters at most. Braces around anything else are text.
 */
const PLACEHOLDER_NAME = /^[A-Za-z][A-Za-z0-9_]{0,31}$/;

/**
 * Splits a filter template at its `{parameter}` placeholders. A brace inside a quoted string
 * is text — `@mt = 'Order {OrderId} shipped'` has no placeholder — which is the server's rule
 * too, so what is highlighted here is what will be bound there.
 */
export function placeholderSegments(filter: string | null | undefined): Segment[] {
  const out: Segment[] = [];
  if (!filter) return out;
  let plain = '';
  const flush = () => { if (plain) { out.push({ text: plain, kind: '' }); plain = ''; } };
  for (let i = 0; i < filter.length; i++) {
    const ch = filter[i];
    if (ch === '\'') {
      // A string literal runs to its closing quote; a backslash keeps the next character.
      let end = i + 1;
      while (end < filter.length && filter[end] !== '\'') end += filter[end] === '\\' ? 2 : 1;
      plain += filter.slice(i, Math.min(end + 1, filter.length));
      i = end;
      continue;
    }
    if (ch === '{') {
      const close = filter.indexOf('}', i + 1);
      const name = close > i ? filter.slice(i + 1, close) : '';
      if (PLACEHOLDER_NAME.test(name)) {
        flush();
        out.push({ text: `{${name}}`, kind: 'param' });
        i = close;
        continue;
      }
    }
    plain += ch;
  }
  flush();
  return out;
}

/** The names of a template's placeholders, each once, in the order they first appear. */
export function placeholderNames(filter: string | null | undefined): string[] {
  const names: string[] = [];
  for (const s of placeholderSegments(filter)) {
    const name = s.text.slice(1, -1);
    if (s.kind === 'param' && !names.includes(name)) names.push(name);
  }
  return names;
}

/**
 * A stored value as an answer's cell would spell it, so the two can be compared.
 *
 * `sent` is the cell it is compared with. The stored value has been through `JSON.parse`, which
 * keeps some sixteen digits of a number, while the cell is the server's exact text: a 64-bit id
 * comes back as `9007199254740992` beside a cell that says `…993`. Where the cell, read the
 * same way, is the very value held here, nothing was hidden and the cell's spelling is the
 * exact one — so that is the answer.
 */
export function cellText(value: unknown, sent?: string): string {
  if (typeof value === 'string') return value;
  if (value === undefined) return '';
  return sent !== undefined && readsAs(sent, value) ? sent : JSON.stringify(value);
}

/** True when a cell's text, read as the stored value was read, is that value. */
function readsAs(sent: string, stored: unknown): boolean {
  if (typeof stored === 'number') return sent.trim() !== '' && Number(sent) === stored;
  // A number inside an object or a list loses its digits the same way.
  if (stored !== null && typeof stored === 'object') {
    try { return JSON.stringify(JSON.parse(sent)) === JSON.stringify(stored); } catch { return false; }
  }
  return false;
}

/**
 * A stored value next to what an answer carries instead of it: the part that differs, with
 * what stands before and after it in both. Null when the two are the same — nothing was hidden.
 */
export function hiddenSpan(stored: string, sent: string): { head: string; stored: string; sent: string; tail: string } | null {
  if (stored === sent) return null;
  const max = Math.min(stored.length, sent.length);
  let head = 0;
  while (head < max && stored[head] === sent[head]) head++;
  let tail = 0;
  while (tail < max - head && stored[stored.length - 1 - tail] === sent[sent.length - 1 - tail]) tail++;
  return {
    head: stored.slice(0, head),
    stored: stored.slice(head, stored.length - tail),
    sent: sent.slice(head, sent.length - tail),
    tail: stored.slice(stored.length - tail),
  };
}

// ── What was hidden ───────────────────────────────────────────────────────────

/** One line of a masking breakdown: what was hidden, what hid it, how many times. */
export interface MaskedLine {
  what: string;
  by: 'field rule' | 'Strict mode' | 'detector';
  count: number;
}

const HIDDEN_WHOLE: Readonly<Record<string, string>> = {
  unscannable: 'unscannable text',
  tooDeep: 'nested too deep',
};

/**
 * A call's masking breakdown as lines, largest first. `detectorNames` turns a detector's id
 * into the name Settings lists it under.
 */
export function maskedLines(by: McpMaskedBy | null | undefined, detectorNames: Readonly<Record<string, string>> = {}): MaskedLine[] {
  const lines: MaskedLine[] = [];
  for (const [what, count] of Object.entries(by?.rules ?? {})) lines.push({ what, by: 'field rule', count });
  for (const [what, count] of Object.entries(by?.strict ?? {})) lines.push({ what, by: 'Strict mode', count });
  for (const [id, count] of Object.entries(by?.detectors ?? {})) {
    lines.push({ what: detectorNames[id] ?? HIDDEN_WHOLE[id] ?? id, by: 'detector', count });
  }
  return lines.sort((a, b) => b.count - a.count || a.what.localeCompare(b.what));
}

/** `6 values replaced: 5 by field rules, 1 by detectors.` */
export function maskedSentence(total: number, groups: { rules: number; strict: number; detectors: number }): string {
  if (total === 0) return 'Nothing in this event is hidden.';
  const parts: string[] = [];
  if (groups.rules) parts.push(`${fmtCount(groups.rules)} by field rules`);
  if (groups.strict) parts.push(`${fmtCount(groups.strict)} by Strict mode`);
  if (groups.detectors) parts.push(`${fmtCount(groups.detectors)} by detectors`);
  return `${fmtCount(total)} ${total === 1 ? 'value' : 'values'} replaced: ${parts.join(', ')}.`;
}

/** The sum of one group of a breakdown. */
export function sumOf(counts: Record<string, number> | null | undefined): number {
  let sum = 0;
  for (const n of Object.values(counts ?? {})) sum += n;
  return sum;
}

// ── Connecting a client ───────────────────────────────────────────────────────

export type McpClientKind = 'claude-code' | 'cursor' | 'vscode' | 'other';

/** A snippet to copy, with the sentence that says where it goes. */
export interface ClientSnippet {
  label: string;
  code: string;
}

/** The variable every snippet reads the key from: the key itself never goes into a file. */
export const KEY_VARIABLE = 'AMETO_MCP_KEY';

/** The protocol revision the endpoint serves without a session. */
export const PROTOCOL_REVISION = '2026-07-28';

/** How to connect a client of this kind to the endpoint at `url`. */
export function clientSnippets(kind: McpClientKind, url: string): ClientSnippet[] {
  switch (kind) {
    case 'claude-code':
      return [
        {
          label: '1. Put the profile key into an environment variable, then run',
          code: `claude mcp add --transport http ameto ${url} \\\n  --header "Authorization: Bearer $${KEY_VARIABLE}"`,
        },
        {
          label: '2. Or share the server with the team: .mcp.json in the repository root',
          code: JSON.stringify({
            mcpServers: { ameto: { type: 'http', url, headers: { Authorization: `Bearer \${${KEY_VARIABLE}}` } } },
          }, null, 2),
        },
      ];
    case 'cursor':
      return [{
        label: '.cursor/mcp.json in the project, or ~/.cursor/mcp.json for every project',
        code: JSON.stringify({
          mcpServers: { ameto: { url, headers: { Authorization: `Bearer \${env:${KEY_VARIABLE}}` } } },
        }, null, 2),
      }];
    case 'vscode':
      return [{
        label: '.vscode/mcp.json in the workspace; VS Code asks for the key once and keeps it',
        code: JSON.stringify({
          inputs: [{ type: 'promptString', id: 'ameto-mcp-key', description: 'AMeto profile key', password: true }],
          servers: { ameto: { type: 'http', url, headers: { Authorization: 'Bearer ${input:ameto-mcp-key}' } } },
        }, null, 2),
      }];
    default:
      return [
        {
          label: 'Any client that speaks Streamable HTTP: this address and one header',
          code: `URL     ${url}\nHeader  Authorization: Bearer <profile key>`,
        },
        {
          // A request of revision 2026-07-28 carries no session: the method is named in a header
          // as well, and the revision and the client's capabilities ride in the request itself.
          label: 'To check a key from a terminal: the tools it is offered',
          code: `curl -s ${url} \\\n  -H "Authorization: Bearer $${KEY_VARIABLE}" \\\n  -H "Content-Type: application/json" \\\n`
              + `  -H "Accept: application/json, text/event-stream" \\\n  -H "MCP-Protocol-Version: ${PROTOCOL_REVISION}" \\\n`
              + `  -H "Mcp-Method: tools/list" \\\n`
              + `  -d '${JSON.stringify({
                jsonrpc: '2.0', id: 1, method: 'tools/list',
                params: { _meta: { 'io.modelcontextprotocol/protocolVersion': PROTOCOL_REVISION, 'io.modelcontextprotocol/clientCapabilities': {} } },
              })}'`,
        },
      ];
  }
}
