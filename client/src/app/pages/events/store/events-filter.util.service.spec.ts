import { EventDto, eventService } from '../../../core/models/event.model';
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
  });

  it('round-trips what it writes', () => {
    const svcs = new Set(['api', 'web']);
    expect(parseServicesFromFilter(setServicesClause("@l = 'Error'", svcs))).toEqual(svcs);
  });
});

describe('eventService', () => {
  const base: EventDto = { '@t': '2026-10-01T09:00:00.0000000Z', '@mt': 'x', '@l': 'Information', id: '1' };

  it('reads @service', () => {
    expect(eventService({ ...base, '@service': 'api' })).toBe('api');
  });

  it('falls back to service.name, the key an older server sent', () => {
    expect(eventService({ ...base, 'service.name': 'legacy' })).toBe('legacy');
  });

  it('prefers @service when both are present', () => {
    expect(eventService({ ...base, '@service': 'api', 'service.name': 'legacy' })).toBe('api');
  });

  it('is undefined for an event without a service', () => {
    expect(eventService(base)).toBeUndefined();
  });
});

it('offers @service in the filter autocomplete', () => {
  expect(BUILTIN_SUGGESTIONS).toContain('@service');
});
