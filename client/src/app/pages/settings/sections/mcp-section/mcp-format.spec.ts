import { describe, expect, it } from 'vitest';

import {
  cellText, clientSnippets, fmtCount, fmtWindow, hasToken, hiddenSpan, lastCall, maskedLines, maskedSentence,
  parseWindow, placeholderNames, placeholderSegments, prettyJson, signatureOf, statusTone, tokenSegments,
} from './mcp-format';

describe('AI / MCP — numbers and windows', () => {
  it('sets thousands apart with a no-break space, and a missing figure is a dash', () => {
    expect(fmtCount(48310)).toBe('48 310');
    expect(fmtCount(1284000)).toBe('1 284 000');
    expect(fmtCount(7)).toBe('7');
    expect(fmtCount(null)).toBe('—');
    expect(fmtCount(undefined)).toBe('—');
  });

  it('writes a window the way a person does, a single day in hours', () => {
    expect(fmtWindow(15)).toBe('15 min');
    expect(fmtWindow(60)).toBe('1 h');
    expect(fmtWindow(1440)).toBe('24 h');
    expect(fmtWindow(10080)).toBe('7 d');
    expect(fmtWindow(90)).toBe('1 h 30 min');
    expect(fmtWindow(0)).toBe('—');
  });

  it('reads back every window it writes', () => {
    for (const minutes of [1, 15, 60, 90, 1440, 4320, 10080]) {
      // "1 h 30 min" is the one spelling the parser does not take: such a value is typed as 90m.
      const text = fmtWindow(minutes);
      if (!text.includes(' h ')) expect(parseWindow(text)).toBe(minutes);
    }
    expect(parseWindow('90m')).toBe(90);
    expect(parseWindow('24h')).toBe(1440);
    expect(parseWindow(' 7 days ')).toBe(10080);
    expect(parseWindow('1w')).toBe(10080);
    expect(parseWindow('1.5h')).toBe(90);
  });

  it('refuses a bare number rather than guess its unit', () => {
    // "48" after "24 h" means hours to the person typing and minutes to the API.
    expect(parseWindow('48')).toBeNull();
    expect(parseWindow('')).toBeNull();
    expect(parseWindow('soon')).toBeNull();
    expect(parseWindow('0h')).toBeNull();
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
});

describe('AI / MCP — a stored value next to what an answer carries', () => {
  it('spells a stored value the way an answer\'s cell does', () => {
    expect(cellText('text')).toBe('text');
    expect(cellText(12500)).toBe('12500');
    expect(cellText(true)).toBe('true');
    expect(cellText(null)).toBe('null');
    expect(cellText({ Iin: '900101300126', Tier: 'gold' })).toBe('{"Iin":"900101300126","Tier":"gold"}');
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

  it('never puts a key into a snippet: each reads it from the environment or asks for it', () => {
    for (const kind of ['claude-code', 'cursor', 'vscode', 'other'] as const) {
      for (const snippet of clientSnippets(kind, url)) {
        expect(snippet.code).toContain(url);
        expect(snippet.code).not.toMatch(/mcp_[0-9a-f]{8}/);
      }
    }
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
