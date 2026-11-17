import { describe, expect, it } from 'vitest';

import { JsonDigits, parseExactJson, writeExactJson } from './exact-json';

/**
 * JSON read and written back with the digits of its whole numbers kept: what a 64-bit id in a
 * log event or in an assistant's arguments needs, where `JSON.parse` would round it.
 */
describe('exact JSON — whole numbers keep their digits', () => {
  it('keeps a whole number past 2^53 as its digits, wherever it stands', () => {
    const text = '{"a":9007199254740993,"b":[18446744073709551615,{"c":-9223372036854775808}],"d":12}';
    const value = parseExactJson(text) as { a: unknown; b: [unknown, { c: unknown }]; d: unknown };
    expect(value.a).toEqual(new JsonDigits('9007199254740993'));
    expect(value.b[0]).toEqual(new JsonDigits('18446744073709551615'));
    expect(value.b[1].c).toEqual(new JsonDigits('-9223372036854775808'));
    // A number a double holds is a number.
    expect(value.d).toBe(12);
    // Written back, it is the text it was read from.
    expect(writeExactJson(value)).toBe(text);
  });

  it('reads everything else as JSON.parse does', () => {
    // Each holds sixteen digits in a row inside a string, so each is walked token by token.
    const long = '"in a string: 9007199254740993"';
    const samples = [
      `{"s":${long},"n":1.5e300,"f":0.1,"t":true,"x":null,"neg":-12,"zero":-0}`,
      `[${long},1,-0,2.5,"\\"quoted\\" and \\\\ back","\\u00e9",[],{},[[]],{"":{}}]`,
      `  { "spaced" : [ 1 , 2 ] , "s" : ${long} }  `,
      long,
    ];
    for (const text of samples) expect(parseExactJson(text)).toEqual(JSON.parse(text));
  });

  it('takes keys as JSON.parse does: the later of two equal ones, and __proto__ as a key', () => {
    expect(parseExactJson('{"k":1,"k":9007199254740993}')).toEqual({ k: new JsonDigits('9007199254740993') });
    expect(parseExactJson('{"k":9007199254740993,"k":2}')).toEqual({ k: 2 });
    const value = parseExactJson('{"__proto__":{"polluted":true},"long":9007199254740993}') as object;
    expect(Object.keys(value)).toEqual(['__proto__', 'long']);
    expect(Object.getPrototypeOf(value)).toBe(Object.prototype);
    expect(({} as Record<string, unknown>)['polluted']).toBeUndefined();
  });

  it('throws on text that is not JSON, as JSON.parse does', () => {
    for (const text of ['', '{', '{"a":9007199254740993', '[9007199254740993,]', 'NaN', "{'a':9007199254740993}"]) {
      expect(() => parseExactJson(text)).toThrow(SyntaxError);
    }
  });

  it('is its digits wherever it turns up outside writeExactJson: in a template, a string, JSON.stringify', () => {
    const id = new JsonDigits('9007199254740993');
    // A template interpolates as String() does: the digits, not "[object Object]".
    expect(String(id)).toBe('9007199254740993');
    expect(`order ${id}`).toBe('order 9007199254740993');
    // JSON.stringify writes the digits in quotes: not the double nearest to them, not an object.
    expect(JSON.stringify({ id, list: [id] })).toBe('{"id":"9007199254740993","list":["9007199254740993"]}');
    // writeExactJson still writes the number itself.
    expect(writeExactJson({ id })).toBe('{"id":9007199254740993}');
  });

  it('lays a value out as JSON.stringify does', () => {
    const value: unknown = JSON.parse('{"a":[1,{"b":[]},{}],"c":"x","d":{"e":null}}');
    expect(writeExactJson(value)).toBe(JSON.stringify(value));
    expect(writeExactJson(value, 2)).toBe(JSON.stringify(value, null, 2));
    expect(writeExactJson({ id: new JsonDigits('9007199254740993'), list: [new JsonDigits('18446744073709551615')] }, 2))
      .toBe('{\n  "id": 9007199254740993,\n  "list": [\n    18446744073709551615\n  ]\n}');
  });
});
