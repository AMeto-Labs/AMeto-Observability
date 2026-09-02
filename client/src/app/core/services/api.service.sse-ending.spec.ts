import { TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { provideRouter } from '@angular/router';
import { ApiService } from './api.service';
import { StreamFrame } from '../models/stream.model';
import { TraceRowDto } from '../models/span.model';
import { EventDto } from '../models/event.model';

/**
 * The terminal `done` frame carries the server's account of WHY a stream ended — and this file
 * is about whether that account survives the trip to a subscriber.
 *
 * <p>It did not. The `done` handler took no argument at all:</p>
 *
 * <pre>es.addEventListener('done', () =&gt; { es.close(); subscriber.complete(); });</pre>
 *
 * <p>so `complete`, `reason` and `truncatedBy` were parsed by the server, written to the wire,
 * and dropped one line before anything could read them. Measured against a running server, the
 * same truncated window then told the user two different stories depending only on the row
 * ceiling the request happened to carry:</p>
 *
 * <pre>
 * ?service=floor-noloss&amp;status=Error&amp;max=50   → done {"complete":false,"reason":"max-rows",
 *                                                     "truncatedBy":"unread-segment"}
 *                                              → the page showed the generic cap hint
 * the same data at max=1000                    → query-error → the page showed a red banner
 * </pre>
 *
 * <p>The three streams differ in what they DO with the ending, and that difference is the other
 * half of what is pinned here: the trace list needs it, and the two log streams must not so
 * much as notice that it now exists.</p>
 */

/** Enough EventSource for the service to wire itself to, and for a test to drive. */
class FakeEventSource {
  static readonly opened: FakeEventSource[] = [];
  private readonly listeners = new Map<string, ((e: unknown) => void)[]>();
  onmessage: ((e: unknown) => void) | null = null;
  onerror: (() => void) | null = null;
  closed = false;

  constructor(readonly url: string) { FakeEventSource.opened.push(this); }

  addEventListener(type: string, fn: (e: unknown) => void): void {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), fn]);
  }
  close(): void { this.closed = true; }

  /** Delivers one frame exactly as EventSource would: `message` on the property, the rest on
   *  their listeners, and nothing at all when nobody registered for the name. */
  emit(type: string, data: string): void {
    if (type === 'message') { this.onmessage?.({ data }); return; }
    for (const fn of this.listeners.get(type) ?? []) fn({ data });
  }
  /** Whether anything is listening for a named frame — `done` on the endless tail is not. */
  listensFor(type: string): boolean { return (this.listeners.get(type) ?? []).length > 0; }

  static get last(): FakeEventSource {
    const es = FakeEventSource.opened[FakeEventSource.opened.length - 1];
    if (!es) throw new Error('no EventSource was opened');
    return es;
  }
}

describe('SSE endings: what the done frame is allowed to tell a subscriber', () => {
  let http: HttpTestingController;
  let api: ApiService;
  const realEventSource = (globalThis as unknown as { EventSource?: unknown }).EventSource;

  beforeEach(() => {
    FakeEventSource.opened.length = 0;
    (globalThis as unknown as { EventSource: unknown }).EventSource = FakeEventSource;
    TestBed.configureTestingModule({
      providers: [provideRouter([]), provideHttpClient(), provideHttpClientTesting()],
    });
    api  = TestBed.inject(ApiService);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    (globalThis as unknown as { EventSource: unknown }).EventSource = realEventSource;
  });

  /** Every stream here is ticketed: the EventSource does not exist until the POST comes back. */
  function grantTicket(): FakeEventSource {
    http.expectOne('/api/auth/sse-ticket').flush({ ticket: 'tk' });
    return FakeEventSource.last;
  }

  /** Records emissions and the completion IN ORDER, which is the point of the ordering test. */
  function watch<T>(source: { subscribe: (o: object) => unknown }): unknown[] {
    const log: unknown[] = [];
    source.subscribe({
      next:     (v: T) => log.push(v),
      complete: () => log.push('COMPLETE'),
      error:    (e: Error) => log.push('ERROR:' + e.message),
    });
    return log;
  }

  it('the trace list is handed the ending, and is handed it BEFORE the completion', () => {
    const log = watch<StreamFrame<TraceRowDto>>(
      api.streamTraceList({ service: 'floor-noloss', status: 'Error' }));
    const es = grantTicket();

    es.emit('message', JSON.stringify({ traceId: 'a' }));
    es.emit('done', '{"complete":false,"reason":"max-rows","truncatedBy":"unread-segment"}');

    // Measured before the fix: [ {traceId:'a'}, 'COMPLETE' ] — the payload never existed here.
    expect(log).toEqual([
      { kind: 'row', row: { traceId: 'a' } },
      { kind: 'end', end: { complete: false, reason: 'max-rows', truncatedBy: 'unread-segment' } },
      'COMPLETE',
    ]);

    // The order is the whole reason the ending travels as a value rather than as a callback:
    // a subscriber reading it in its `complete` handler has already been given it.
    expect(log.indexOf('COMPLETE')).toBe(log.length - 1);
    expect(es.closed).toBe(true);
  });

  it('an ending that explains nothing is an empty account, never a thrown one', () => {
    for (const payload of ['{}', 'not json at all', '[1,2,3]', 'null', '"exhausted"']) {
      FakeEventSource.opened.length = 0;
      const log = watch<StreamFrame<TraceRowDto>>(api.streamTraceList({}));
      const es  = grantTicket();
      es.emit('done', payload);

      // Every unreadable shape lands on the same answer — no fields, so no field is `false`
      // by accident — and the stream still completes normally. A payload this client cannot
      // read is an ending it cannot describe, not an ending that did not happen.
      expect(log, `payload ${payload}`).toEqual([{ kind: 'end', end: {} }, 'COMPLETE']);
    }
  });

  it('a `done` with no data at all still completes the stream', () => {
    const log = watch<StreamFrame<TraceRowDto>>(api.streamTraceQuery({ query: '{ status = error }' }));
    const es  = grantTicket();
    // MessageEvent.data is typed string, but a frame that never carried one arrives undefined.
    (es as unknown as { emit(t: string, d: unknown): void }).emit('done', undefined);
    expect(log).toEqual([{ kind: 'end', end: {} }, 'COMPLETE']);
  });

  it('the historical log stream is unchanged: rows only, and it still completes on done', () => {
    const log = watch<EventDto>(api.streamEvents({ filter: 'x' }));
    const es  = grantTicket();

    es.emit('message', JSON.stringify({ id: '1' }));
    es.emit('done', '{"complete":true,"reason":"exhausted"}');

    // The ending frame is filtered out for a caller with nowhere to put one — the events store
    // subscribes to this and would have taken an ending for a row.
    expect(log).toEqual([{ id: '1' }, 'COMPLETE']);
  });

  it('the live tail is unchanged: it does not even listen for done, so it cannot complete', () => {
    const log = watch<EventDto>(api.streamLive({ filter: 'x' }));
    const es  = grantTicket();

    es.emit('message', JSON.stringify({ id: '1' }));
    // Registering the listener at all would be the regression: the tail has no end, and a
    // server that ever sent `done` must not be able to stop it.
    expect(es.listensFor('done')).toBe(false);
    es.emit('done', '{"complete":true,"reason":"exhausted"}');

    expect(log).toEqual([{ id: '1' }]);
    expect(es.closed).toBe(false);
  });
});
