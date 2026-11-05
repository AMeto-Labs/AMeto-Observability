import { describe, it, expect } from 'vitest';

import { EventDto } from '../../../../core/models/event.model';
import { buildClefView, buildProps, countJsonHits } from './event-detail';

/**
 * The detail panel shows the service once, in its own row, as the header field `@service`. A
 * property that merely REPEATS it — what an older server stored for every OTLP event — is hidden;
 * a property that says something else is the user's data and stays. And in the JSON tab, where
 * `@service` means the header exactly as it does in a filter, no property may replace it.
 */
describe('Event detail — the service and the properties beside it', () => {
  const base: EventDto = { '@t': '2026-10-01T09:00:00.0000000Z', '@mt': 'x', '@l': 'Information', id: '1' };
  const labels = (props: Record<string, unknown>, service?: string): string[] =>
    buildProps(props, service).map(p => p.label);

  it('hides a service.name property that is a copy of the header', () => {
    expect(labels({ 'service.name': 'api', Region: 'eu' }, 'api')).toEqual(['Region']);
  });

  it('keeps a service.name property that is not the header', () => {
    expect(labels({ 'service.name': 'from-record', Region: 'eu' }, 'api')).toEqual(['service.name', 'Region']);
    expect(labels({ 'service.name': 7 }, 'api')).toEqual(['service.name']);
    expect(labels({ 'service.name': 'api' }, undefined)).toEqual(['service.name']);
  });

  it('treats a legacy @service property the same way', () => {
    expect(labels({ '@service': 'api' }, 'api')).toEqual([]);
    expect(labels({ '@service': 'rogue' }, 'api')).toEqual(['@service']);
  });

  it('still hides the trace and span copies', () => {
    expect(labels({ '@tr': 'abc', '@sp': 'def', n: 1 }, 'api')).toEqual(['n']);
  });

  it('the JSON tab shows the header @service, among the header fields, whatever the properties say', () => {
    const view = buildClefView({ ...base, '@service': 'api', props: { '@service': 'rogue', n: 1 } });
    expect(view['@service']).toBe('api');
    expect(Object.keys(view)).toEqual(['@t', '@l', '@mt', '@service', 'n']);
  });

  it('the phone\'s JSON toolbar counts the rows a search lights — a key or a leaf value, once each', () => {
    const view = { PaymentId: 'pay_7Qk2', Http: { Path: '/payments/pay_7Qk2', Status: 504 }, Tags: ['payments', 'x'], Retry: null };
    expect(countJsonHits(view, 'pay')).toBe(3);   // PaymentId (key and value: one row), Http.Path, Tags[0]
    expect(countJsonHits(view, 'null')).toBe(1);
    expect(countJsonHits(view, 'http')).toBe(1);   // the container's own row
    expect(countJsonHits(view, '')).toBe(0);
    expect(countJsonHits(view, 'nothing')).toBe(0);
  });

  it('the JSON tab takes the service from @service alone, not from the key servers before it sent', () => {
    // A server older than @service sent the service as a top-level `service.name`. The client is
    // built into the wwwroot of the server it ships with, so it never meets one: no header field.
    const view = buildClefView({ ...base, 'service.name': 'legacy' } as EventDto);
    expect('@service' in view).toBe(false);
    expect('service.name' in view).toBe(false);
  });
});
