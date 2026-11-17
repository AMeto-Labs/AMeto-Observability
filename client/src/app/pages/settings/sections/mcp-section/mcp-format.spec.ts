import { describe, expect, it } from 'vitest';

import { parseExactJson } from '../../../../shared/utils/exact-json';
import {
  KEY_VARIABLE, asCount, cellText, clientSnippets, fmtCount, fmtWindow, hasToken, hiddenSpan, isCount, lastCall, maskedLines,
  maskedSentence, parseWindow, placeholderNames, placeholderSegments, prettyJson, sameWindow, signatureOf, statusTone,
  tokenSegments,
} from './mcp-format';

describe('AI / MCP — numbers and windows', () => {
  it('sets thousands apart with a no-break space, and a missing figure is a dash', () => {
    expect(fmtCount(48310)).toBe('48 310');
    expect(fmtCount(1284000)).toBe('1 284 000');
    expect(fmtCount(7)).toBe('7');
    expect(fmtCount(null)).toBe('—');
    expect(fmtCount(undefined)).toBe('—');
  });

  it('writes a window as one number and one unit, a single day in hours', () => {
    expect(fmtWindow(15)).toBe('15 min');
    expect(fmtWindow(60)).toBe('1 h');
    expect(fmtWindow(1440)).toBe('24 h');
    expect(fmtWindow(10080)).toBe('7 d');
    // No whole number of hours: minutes, not "1 h 30 min" — the form that shows it has to save it.
    expect(fmtWindow(90)).toBe('90 min');
    expect(fmtWindow(1441)).toBe('1441 min');
    expect(fmtWindow(0)).toBe('—');
  });

  it('reads back every window it writes', () => {
    // Every value a window can hold: one minute to the server's ceiling of 366 days. A form is
    // filled with fmtWindow and saved through parseWindow, so one value that does not come back
    // is a form that cannot be saved until its window is typed again.
    const lost: string[] = [];
    for (let minutes = 1; minutes <= 366 * 1440; minutes++) {
      const text = fmtWindow(minutes);
      if (parseWindow(text) !== minutes) lost.push(`${minutes} → "${text}" → ${parseWindow(text)}`);
    }
    expect(lost).toEqual([]);
  });

  it('reads the spellings a person types', () => {
    expect(parseWindow('90m')).toBe(90);
    expect(parseWindow('24h')).toBe(1440);
    expect(parseWindow(' 7 days ')).toBe(10080);
    expect(parseWindow('1w')).toBe(10080);
    expect(parseWindow('1.5h')).toBe(90);
    expect(parseWindow('1,5 h')).toBe(90);
    expect(parseWindow('45 MIN')).toBe(45);
  });

  it('reads the compound form too: units from the largest down, each once', () => {
    expect(parseWindow('1 h 30 min')).toBe(90);
    expect(parseWindow('1h30m')).toBe(90);
    expect(parseWindow('1 hour 30 minutes')).toBe(90);
    expect(parseWindow('1d 12h')).toBe(2160);
    expect(parseWindow('1w 2d 3h 4m')).toBe(10080 + 2880 + 180 + 4);
    // Not a window: the units out of order, one of them twice, a part without its unit.
    expect(parseWindow('30m 1h')).toBeNull();
    expect(parseWindow('1h 1h')).toBeNull();
    expect(parseWindow('1h 30')).toBeNull();
    expect(parseWindow('1h, 30m')).toBeNull();
  });

  it('takes two spellings of one window for the same window', () => {
    // A form filled with "24 h" and holding "24h" has nothing unsaved.
    expect(sameWindow('24 h', '24h')).toBe(true);
    expect(sameWindow('90 min', '1h30m')).toBe(true);
    expect(sameWindow('1 d', '24 hours')).toBe(true);
    expect(sameWindow('24 h', '25 h')).toBe(false);
    // What is no window is the same only as the same text.
    expect(sameWindow('soon', 'soon')).toBe(true);
    expect(sameWindow('soon', 'later')).toBe(false);
    expect(sameWindow('48', '48 min')).toBe(false);
  });

  it('refuses a bare number rather than guess its unit', () => {
    // "48" after "24 h" means hours to the person typing and minutes to the API.
    expect(parseWindow('48')).toBeNull();
    expect(parseWindow('')).toBeNull();
    expect(parseWindow('   ')).toBeNull();
    expect(parseWindow('soon')).toBeNull();
    expect(parseWindow('0h')).toBeNull();
    expect(parseWindow('5 months')).toBeNull();
  });

  it('keeps an emptied number field empty, and says apart whether a count will do', () => {
    // The field's value accessor answers null for an empty field: that is not a zero.
    expect(asCount(null)).toBeNull();
    expect(asCount('')).toBeNull();
    expect(asCount(undefined)).toBeNull();
    expect(asCount('abc')).toBeNull();
    expect(asCount(300)).toBe(300);
    expect(asCount('300')).toBe(300);
    expect(asCount(12.7)).toBe(12);
    // What was typed stays as typed; whether it will do is asked on save.
    expect(asCount(0)).toBe(0);
    expect(asCount(-5)).toBe(-5);

    expect(isCount(300)).toBe(true);
    for (const value of [null, undefined, 0, -5, 1.5, Number.NaN]) expect(isCount(value)).toBe(false);
  });

  it('says when a profile last called: the gap, then the clock, then the day', () => {
    const now = new Date(2026, 10, 16, 15, 0, 0).getTime();
    const ago = (ms: number) => new Date(now - ms).toISOString();
    expect(lastCall(null, now)).toBe('never');
    expect(lastCall(ago(20_000), now)).toBe('just now');
    expect(lastCall(ago(2 * 60_000), now)).toBe('2 min ago');
    expect(lastCall(ago(3 * 3_600_000), now)).toBe('3 h ago');
    expect(lastCall(ago(8 * 3_600_000), now)).toBe('07:00');
    expect(lastCall(ago(20 * 3_600_000), now)).toBe('yesterday');
    expect(lastCall(ago(5 * 86_400_000), now)).toBe('2026-11-11');
  });
});

describe('AI / MCP — tools', () => {
  const schema = {
    properties: { merchantId: {}, reason: {}, from: {}, to: {}, cursor: {} },
    required: ['merchantId'],
  };

  it('writes a tool\'s arguments in one line, the optional ones with a question mark', () => {
    expect(signatureOf(schema)).toBe('merchantId, reason?, from?, to?, cursor?');
    expect(signatureOf(schema, true, ' · ')).toBe('merchantId · reason?');
    expect(signatureOf(null)).toBe('');
  });

  it('gives a status its tone', () => {
    expect(statusTone('OK')).toBe('ok');
    expect(statusTone('Trimmed')).toBe('warn');
    for (const status of ['Refused', 'Timed out', 'Failed']) expect(statusTone(status)).toBe('err');
    expect(statusTone(null)).toBe('');
  });

  it('lays JSON out for reading and leaves anything else alone', () => {
    expect(prettyJson('{"filter":"@l = \'Error\'","limit":5}')).toBe('{\n  "filter": "@l = \'Error\'",\n  "limit": 5\n}');
    expect(prettyJson('{"cut off…')).toBe('{"cut off…');
    expect(prettyJson(null)).toBe('');
  });

  it('shows a 64-bit argument with the digits it was sent with', () => {
    // The filter under the arguments says …993: the arguments must not say …992.
    expect(prettyJson('{"orderId":9007199254740993,"ids":[18446744073709551615,-9223372036854775808],"limit":5}')).toBe(
      '{\n  "orderId": 9007199254740993,\n  "ids": [\n    18446744073709551615,\n    -9223372036854775808\n  ],\n  "limit": 5\n}');
  });
});

describe('AI / MCP — tokens and placeholders', () => {
  it('splits a text at its masking tokens', () => {
    expect(tokenSegments("CardNumber = 'h:91c0e2aa5d17' and @l = 'Error'")).toEqual([
      { text: "CardNumber = '", kind: '' },
      { text: 'h:91c0e2aa5d17', kind: 'token' },
      { text: "' and @l = 'Error'", kind: '' },
    ]);
    expect(tokenSegments('nothing hidden')).toEqual([{ text: 'nothing hidden', kind: '' }]);
    expect(tokenSegments(null)).toEqual([]);
    // Twelve hex digits exactly: a shorter run is not a token.
    expect(hasToken('h:91c0e2aa5d1')).toBe(false);
    expect(hasToken('x h:91c0e2aa5d17 y')).toBe(true);
  });

  it('finds placeholders outside quoted strings only — the server\'s rule', () => {
    const filter = "@mt = 'Order {OrderId} and {more}' and Owner = {owner} and Tags in ['a {b}', {tag}]";
    expect(placeholderNames(filter)).toEqual(['owner', 'tag']);
    expect(placeholderSegments(filter).filter(s => s.kind === 'param').map(s => s.text)).toEqual(['{owner}', '{tag}']);
    // Nothing is lost or reordered by the split.
    expect(placeholderSegments(filter).map(s => s.text).join('')).toBe(filter);
  });

  it('keeps a quote escaped with a backslash inside its string', () => {
    const filter = "Owner = 'o\\'brien {x}' and Id = {id}";
    expect(placeholderNames(filter)).toEqual(['id']);
    expect(placeholderSegments(filter).map(s => s.text).join('')).toBe(filter);
  });

  it('names each placeholder once and passes over braces that are not one', () => {
    expect(placeholderNames('A = {a} or B = {a} or C = {b}')).toEqual(['a', 'b']);
    expect(placeholderNames('A = {} and B = {1x} and C = {not closed')).toEqual([]);
    expect(placeholderNames('')).toEqual([]);
  });

  it('takes for a placeholder exactly what the server binds: a letter first, 32 characters at most', () => {
    // FilterTemplate.IsIdentifier on the server: ^[A-Za-z][A-Za-z0-9_]{0,31}$. A name coloured
    // here and passed over there would look bound and be sent to the engine as text.
    const longest = 'a' + '0'.repeat(31);
    expect(placeholderNames(`A = {x} and B = {merchant_Id2} and C = {${longest}}`)).toEqual(['x', 'merchant_Id2', longest]);
    expect(placeholderNames(`A = {_x} and B = {${longest}0} and C = {a-b} and D = {a b}`)).toEqual([]);
  });
});

describe('AI / MCP — a stored value next to what an answer carries', () => {
  it('spells a stored value the way an answer\'s cell does', () => {
    expect(cellText('text')).toBe('text');
    expect(cellText(12500)).toBe('12500');
    expect(cellText(true)).toBe('true');
    expect(cellText(null)).toBe('null');
    expect(cellText({ Iin: '900101300126', Tier: 'gold' })).toBe('{"Iin":"900101300126","Tier":"gold"}');
  });

  it('keeps the digits of a 64-bit number, hidden or not', () => {
    // A 64-bit id as the server writes it, read as the preview reads the stored event.
    const exact = '9007199254740993';
    const stored = parseExactJson(exact);
    // The cell is the same number: nothing was hidden.
    expect(cellText(stored, exact)).toBe(exact);
    expect(hiddenSpan(cellText(stored, exact), exact)).toBeNull();
    // Hidden, it is shown as stored — not as the double nearest to it.
    expect(cellText(stored, 'h:91c0e2aa5d17')).toBe(exact);
    // The same inside an object, whichever way the server spaced or escaped its cell.
    const order = parseExactJson(`{"Id":${exact},"Tier":"gold"}`);
    expect(cellText(order, `{ "Id": ${exact}, "Tier": "\\u0067old" }`)).toBe(`{ "Id": ${exact}, "Tier": "\\u0067old" }`);
    expect(cellText(order, '{"Id":"h:91c0e2aa5d17","Tier":"gold"}')).toBe(`{"Id":${exact},"Tier":"gold"}`);
    // Another number of the same double is another number.
    expect(cellText(stored, '9007199254740992')).toBe(exact);
  });

  it('marks what differs from the cell, whatever the cell says', () => {
    // A cell that is not the stored value stays what was hidden: a token, a kept tail, another number.
    expect(cellText(4111111111111111, '****1111')).toBe('4111111111111111');
    expect(cellText(12500, '12501')).toBe('12500');
    expect(cellText(0, '')).toBe('0');
    // The cell's own spelling of the same number is taken.
    expect(cellText(12500, '12500.0')).toBe('12500.0');
    // A stored string is its own text, whatever the cell says.
    expect(cellText('12500', '12500.0')).toBe('12500');
  });

  it('finds the part that differs, with what stands around it', () => {
    expect(hiddenSpan('callback to a.serikov@example.com failed', 'callback to h:3aa0715ce9d4 failed')).toEqual({
      head: 'callback to ', stored: 'a.serikov@example.com', sent: 'h:3aa0715ce9d4', tail: ' failed',
    });
    expect(hiddenSpan('4111111111111111', 'h:91c0e2aa5d17')).toEqual({
      head: '', stored: '4111111111111111', sent: 'h:91c0e2aa5d17', tail: '',
    });
    expect(hiddenSpan('5555555555554444', '****4444')).toEqual({ head: '', stored: '555555555555', sent: '****', tail: '4444' });
    expect(hiddenSpan('insufficient_funds', 'insufficient_funds')).toBeNull();
  });
});

describe('AI / MCP — what was hidden', () => {
  it('lists a breakdown by what did the hiding, largest first, detectors by their names', () => {
    expect(maskedLines(
      { rules: { CardNumber: 50, 'Customer.Iin': 9 }, strict: { Amount: 12 }, detectors: { email: 9, tooDeep: 1 }, ruleIds: { r1: 50 } },
      { email: 'Email address' },
    )).toEqual([
      { what: 'CardNumber', by: 'field rule', count: 50 },
      { what: 'Amount', by: 'Strict mode', count: 12 },
      { what: 'Customer.Iin', by: 'field rule', count: 9 },
      { what: 'Email address', by: 'detector', count: 9 },
      { what: 'nested too deep', by: 'detector', count: 1 },
    ]);
    expect(maskedLines(null)).toEqual([]);
  });

  it('keeps a field and a detector of one name apart', () => {
    expect(maskedLines({ rules: { email: 2 }, detectors: { email: 3 } }, { email: 'Email address' })).toEqual([
      { what: 'Email address', by: 'detector', count: 3 },
      { what: 'email', by: 'field rule', count: 2 },
    ]);
  });

  it('says in a sentence how many values were replaced and by what', () => {
    expect(maskedSentence(6, { rules: 5, strict: 0, detectors: 1 })).toBe('6 values replaced: 5 by field rules, 1 by detectors.');
    expect(maskedSentence(1, { rules: 0, strict: 1, detectors: 0 })).toBe('1 value replaced: 1 by Strict mode.');
    expect(maskedSentence(0, { rules: 0, strict: 0, detectors: 0 })).toBe('Nothing in this event is hidden.');
  });
});

describe('AI / MCP — connecting a client', () => {
  const url = 'https://ameto.example.com/ameto/mcp';

  it('points every snippet at the endpoint, and its bearer at where the key is kept', () => {
    // What follows "Bearer" in a snippet is a reference, never a value: the variable in the
    // shell's or the editor's own syntax, VS Code's prompt, or — where there is no file to put
    // a reference in — a placeholder to be replaced by hand.
    const references = [`$${KEY_VARIABLE}`, `\${${KEY_VARIABLE}}`, `\${env:${KEY_VARIABLE}}`, '${input:ameto-mcp-key}', '<profile key>'];
    let bearers = 0;
    for (const kind of ['claude-code', 'cursor', 'vscode', 'other'] as const) {
      for (const snippet of clientSnippets(kind, url)) {
        expect(snippet.code).toContain(url);
        for (const [, bearer] of snippet.code.matchAll(/Bearer ([^"\n]+)/g)) {
          expect(references).toContain(bearer);
          bearers++;
        }
      }
    }
    // Every snippet carries the header: six of them over the four kinds of client.
    expect(bearers).toBe(6);
  });

  it('writes the command and the shared file for Claude Code', () => {
    const [cli, file] = clientSnippets('claude-code', url);
    expect(cli.code).toBe(`claude mcp add --transport http ameto ${url} \\\n  --header "Authorization: Bearer $AMETO_MCP_KEY"`);
    expect(JSON.parse(file.code)).toEqual({
      mcpServers: { ameto: { type: 'http', url, headers: { Authorization: 'Bearer ${AMETO_MCP_KEY}' } } },
    });
  });

  it('writes each editor\'s own file format', () => {
    expect(JSON.parse(clientSnippets('cursor', url)[0].code).mcpServers.ameto.headers.Authorization).toBe('Bearer ${env:AMETO_MCP_KEY}');
    const vscode = JSON.parse(clientSnippets('vscode', url)[0].code);
    expect(vscode.servers.ameto).toEqual({ type: 'http', url, headers: { Authorization: 'Bearer ${input:ameto-mcp-key}' } });
    expect(vscode.inputs[0]).toMatchObject({ id: 'ameto-mcp-key', password: true });
  });

  it('gives a terminal check that is a whole request of the sessionless revision', () => {
    const check = clientSnippets('other', url)[1].code;
    // The server refuses the request without any one of these (seen against a running server).
    expect(check).toContain('-H "MCP-Protocol-Version: 2026-07-28"');
    expect(check).toContain('-H "Mcp-Method: tools/list"');
    expect(check).toContain('-H "Accept: application/json, text/event-stream"');
    const body = JSON.parse(check.slice(check.indexOf("-d '") + 4, check.lastIndexOf("'")));
    expect(body.method).toBe('tools/list');
    expect(body.params._meta).toEqual({
      'io.modelcontextprotocol/protocolVersion': '2026-07-28',
      'io.modelcontextprotocol/clientCapabilities': {},
    });
  });
});
