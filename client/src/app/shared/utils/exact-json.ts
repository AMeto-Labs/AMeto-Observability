/**
 * JSON whose whole numbers keep their digits.
 *
 * `JSON.parse` makes every number a double, which holds a whole number exactly only up to 2^53:
 * a 64-bit id comes back as another number — `9007199254740993` as `…992` — and is written
 * back with the other digits. Read here, a whole number past 2^53 is kept as the digits it was
 * written with ({@link JsonDigits}), and written back with them; every other value is what
 * `JSON.parse` makes of it.
 */

/** A whole number past what a double holds exactly, as the digits it was written with. */
export class JsonDigits {
  constructor(readonly text: string) {}
}

/** One token of JSON text, after the space before it: a string, a number, a literal or a mark. */
const TOKEN = /\s*("(?:[^"\\]|\\[\s\S])*"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null|[{}[\]:,])/y;

/** Sixteen digits in a row: text without them holds no number a double could change. */
const LONG_RUN = /\d{16}/;

const WHOLE = /^-?\d+$/;

/**
 * `JSON.parse`, except that a whole number past 2^53 is a {@link JsonDigits}. Text that is not
 * JSON throws as it does for `JSON.parse` — which reads it first, so what follows here only
 * ever walks text it has accepted.
 */
export function parseExactJson(text: string): unknown {
  const plain: unknown = JSON.parse(text);
  if (!LONG_RUN.test(text)) return plain;

  const tokens: string[] = [];
  TOKEN.lastIndex = 0;
  for (let m = TOKEN.exec(text); m; m = TOKEN.exec(text)) tokens.push(m[1]);

  let at = 0;
  const value = (): unknown => {
    const token = tokens[at++];
    if (token === '{') {
      const entries: [string, unknown][] = [];
      if (tokens[at] === '}') at++;
      else {
        do {
          const key = JSON.parse(tokens[at]) as string;
          at += 2; // the key, and the colon after it
          entries.push([key, value()]);
        } while (tokens[at++] === ',');
      }
      // As JSON.parse has it: a key called __proto__ is a key like any other, and of two equal
      // keys the later one holds.
      return Object.fromEntries(entries);
    }
    if (token === '[') {
      const items: unknown[] = [];
      if (tokens[at] === ']') at++;
      else {
        do items.push(value()); while (tokens[at++] === ',');
      }
      return items;
    }
    return WHOLE.test(token) && !Number.isSafeInteger(Number(token)) ? new JsonDigits(token) : JSON.parse(token);
  };
  return value();
}

/**
 * `JSON.stringify(value, null, indent)`, with a {@link JsonDigits} written as its own digits.
 * Meant for what {@link parseExactJson} reads: strings, numbers, booleans, null, lists, objects.
 */
export function writeExactJson(value: unknown, indent = 0): string {
  const step = ' '.repeat(indent);
  const write = (v: unknown, pad: string): string => {
    if (v instanceof JsonDigits) return v.text;
    if (v === null || typeof v !== 'object') return JSON.stringify(v) ?? 'null';
    const inner = pad + step;
    const [open, close, parts] = Array.isArray(v)
      ? ['[', ']', v.map(item => write(item, inner))]
      : ['{', '}', Object.entries(v).map(([key, item]) => `${JSON.stringify(key)}:${step ? ' ' : ''}${write(item, inner)}`)];
    if (parts.length === 0) return open + close;
    return step ? `${open}\n${inner}${parts.join(`,\n${inner}`)}\n${pad}${close}` : `${open}${parts.join(',')}${close}`;
  };
  return write(value, '');
}
