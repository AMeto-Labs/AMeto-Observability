import { BUILTIN_SUGGESTIONS, parseServicesFromFilter, setServicesClause } from './events-filter.util';

/**
 * The service is the server's built-in `@service` field, and the events page now writes its
 * service clause in that spelling. The page wrote `['service.name'] = …` before, and that form
 * lives on in saved searches, search history and shared URLs — so the picker must still READ it
 * and must REPLACE it rather than add a second, contradictory clause beside it.
 */
describe('the service clause the events page writes', () => {
  it('is @service = … for one service', () => {
    expect(setServicesClause('', new Set(['api']))).toBe("@service = 'api'");
  });

  it('is @service in […] for several', () => {
    expect(setServicesClause('', new Set(['api', 'web']))).toBe("@service in ['api', 'web']");
  });

  it('goes after the level clause and before the rest of the expression', () => {
    expect(setServicesClause("@l = 'Error' and Region = 'eu'", new Set(['api'])))
      .toBe("@l = 'Error' and @service = 'api' and Region = 'eu'");
  });

  it('replaces its own clause', () => {
    expect(setServicesClause("@service = 'old' and Region = 'eu'", new Set(['a', 'b'])))
      .toBe("@service in ['a', 'b'] and Region = 'eu'");
  });

  it('replaces a clause in the older bracket spelling instead of adding a second one', () => {
    expect(setServicesClause("['service.name'] = 'old' and Region = 'eu'", new Set(['api'])))
      .toBe("@service = 'api' and Region = 'eu'");
    expect(setServicesClause("@l = 'Error' and ['service.name'] in ['a', 'b']", new Set(['api'])))
      .toBe("@l = 'Error' and @service = 'api'");
  });

  it('clears either spelling when nothing is selected', () => {
    expect(setServicesClause("@service in ['a', 'b'] and Region = 'eu'", new Set())).toBe("Region = 'eu'");
    expect(setServicesClause("['service.name'] = 'a'", new Set())).toBe('');
  });

  it('leaves a negated service test as the user wrote it', () => {
    expect(setServicesClause("not @service = 'x' and Region = 'eu'", new Set(['a'])))
      .toBe("@service = 'a' and not @service = 'x' and Region = 'eu'");
    expect(setServicesClause("not @service = 'x'", new Set())).toBe("not @service = 'x'");
  });

  it('ANDs the selection with a whole top-level or, instead of tearing a branch out of it', () => {
    expect(setServicesClause("@service = 'a' or @l = 'Fatal'", new Set(['b'])))
      .toBe("@service = 'b' and (@service = 'a' or @l = 'Fatal')");
    expect(setServicesClause("@service = 'a' or @l = 'Fatal'", new Set())).toBe("@service = 'a' or @l = 'Fatal'");
  });

  it('does not split at an and inside quotes, brackets or parentheses', () => {
    expect(setServicesClause("Msg = 'x and y' and @service = 'a'", new Set(['b'])))
      .toBe("@service = 'b' and Msg = 'x and y'");
    expect(setServicesClause("(A = 1 and B = 2) and @service = 'a'", new Set()))
      .toBe('(A = 1 and B = 2)');
  });

  it('matches the field ordinally, as the server does: @SERVICE is a user property', () => {
    expect(setServicesClause("@SERVICE = 'x'", new Set(['a']))).toBe("@service = 'a' and @SERVICE = 'x'");
  });

  it('writes a quote or a backslash in a service name escaped, so the server reads the name', () => {
    // The lexer ends a string at a lone quote and drops a backslash before the next character.
    expect(setServicesClause('', new Set(["O'Brien"]))).toBe("@service = 'O''Brien'");
    expect(setServicesClause('', new Set(['DOMAIN\\svc', "it's"])))
      .toBe("@service in ['DOMAIN\\\\svc', 'it''s']");
  });
});

describe('reading the selected services back out of a filter', () => {
  it('reads the @service spelling', () => {
    expect([...parseServicesFromFilter("@service = 'api'")]).toEqual(['api']);
    expect([...parseServicesFromFilter("@l = 'Error' and @service in ['a', 'b']")]).toEqual(['a', 'b']);
  });

  it('still reads the older bracket spelling', () => {
    expect([...parseServicesFromFilter("['service.name'] = 'api'")]).toEqual(['api']);
    expect([...parseServicesFromFilter("['service.name'] in ['a', 'b']")]).toEqual(['a', 'b']);
  });

  it('does not read an exclusion as a selection', () => {
    expect(parseServicesFromFilter("@service <> 'api'").size).toBe(0);
    expect(parseServicesFromFilter("@service != 'api'").size).toBe(0);
    expect(parseServicesFromFilter("not @service = 'api'").size).toBe(0);
  });

  it('reads nothing as a selection beside a top-level or', () => {
    expect(parseServicesFromFilter("@service = 'a' or @l = 'Fatal'").size).toBe(0);
    expect(parseServicesFromFilter("@l = 'Fatal' or @service in ['a', 'b']").size).toBe(0);
  });

  it('reads a selection inside an and that nests an or', () => {
    expect([...parseServicesFromFilter("(@l = 'Error' or @l = 'Fatal') and @service = 'a'")]).toEqual(['a']);
  });

  it('matches the field ordinally and the in keyword in any case', () => {
    expect(parseServicesFromFilter("@SERVICE = 'x'").size).toBe(0);
    expect([...parseServicesFromFilter("@service IN ['a']")]).toEqual(['a']);
  });

  it('round-trips what it writes', () => {
    const svcs = new Set(['api', 'web']);
    expect(parseServicesFromFilter(setServicesClause("@l = 'Error'", svcs))).toEqual(svcs);
  });
});

/**
 * The server answers the field under four names — BuiltinFields' ServiceName row, matched
 * ordinally: `@service`, `ServiceName`, the bare dotted `service.name` and the bracketed
 * `['service.name']`. The picker knew the first and the last only, so a filter in either of the
 * other two — `ServiceName = 'x'` was the documented example until #113 — kept its clause as the
 * user's text, and a pick ANDed `@service = 'new'` in front of it: two service clauses that
 * contradict each other, and an empty page.
 */
describe('the service clause under every name the server answers for it', () => {
  const SPELLINGS = ['@service', 'ServiceName', 'service.name', "['service.name']"];

  it('reads each spelling as the selection', () => {
    for (const f of SPELLINGS) {
      expect([...parseServicesFromFilter(`${f} = 'api'`)], f).toEqual(['api']);
      expect([...parseServicesFromFilter(`@l = 'Error' and ${f} in ['a', 'b']`)], f)
        .toEqual(['a', 'b']);
    }
  });

  it('replaces each spelling with the pick, and clears it when nothing is picked', () => {
    for (const f of SPELLINGS) {
      expect(setServicesClause(`${f} = 'old' and Region = 'eu'`, new Set(['new'])), f)
        .toBe("@service = 'new' and Region = 'eu'");
      expect(setServicesClause(`@l = 'Error' and ${f} in ['a', 'b']`, new Set()), f)
        .toBe("@l = 'Error'");
    }
  });

  it("turns a saved ServiceName = 'old' and a pick into one clause, not two that contradict", () => {
    const saved = "ServiceName = 'old' and Region = 'eu'";
    const opened = parseServicesFromFilter(saved);          // what the picker opens with
    expect([...opened]).toEqual(['old']);
    expect(setServicesClause(saved, new Set([...opened, 'new'])))
      .toBe("@service in ['old', 'new'] and Region = 'eu'");
    expect(setServicesClause(saved, new Set(['new']))).toBe("@service = 'new' and Region = 'eu'");
  });

  it('repairs, at the next pick, a contradiction the old picker already saved', () => {
    const saved = "@service = 'new' and ServiceName = 'old' and Region = 'eu'";
    expect([...parseServicesFromFilter(saved)]).toEqual(['new']);   // it opens with the first
    expect(setServicesClause(saved, new Set(['new']))).toBe("@service = 'new' and Region = 'eu'");
  });

  it('leaves alone what only looks like the field: other properties, other cases, other tests', () => {
    for (const other of [
      "service.namespace = 'x'", "['service.namespace'] = 'x'", "service.name.id = 'x'",
      "@service.name = 'x'", "ServiceNameX = 'x'", "MyServiceName = 'x'",
      "servicename = 'x'", "SERVICENAME = 'x'", "Service.Name = 'x'", "['Service.Name'] = 'x'",
      "ServiceName <> 'x'", "service.name like 'x%'", "not ServiceName = 'x'",
    ]) {
      expect(parseServicesFromFilter(other).size, other).toBe(0);
      expect(setServicesClause(other, new Set(['a'])), other).toBe(`@service = 'a' and ${other}`);
    }
  });
});

it('offers @service in the filter autocomplete', () => {
  expect(BUILTIN_SUGGESTIONS).toContain('@service');
});
