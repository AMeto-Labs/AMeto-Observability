import { TestBed } from '@angular/core/testing';
import { provideZonelessChangeDetection } from '@angular/core';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting, HttpTestingController } from '@angular/common/http/testing';
import { LUCIDE_ICONS, LucideIconProvider, icons } from 'lucide-angular';
import { describe, it, expect, beforeEach } from 'vitest';

import { SettingsDirtyService } from './settings-dirty.service';
import { EventsSectionComponent } from './sections/events-section/events-section';
import { RetentionSectionComponent } from './sections/retention-section/retention-section';
import { UsersSectionComponent } from './sections/users-section/users-section';
import { ApiKeysSectionComponent } from './sections/api-keys-section/api-keys-section';

/**
 * A settings page nobody has typed into must not claim unsaved changes.
 *
 * The symptom this pins: leaving Settings always raised the canDeactivate confirm, even on a
 * visit where nothing was touched. The guard reads one aggregated flag, so any single section
 * reporting dirty on mount is enough to produce it — which makes "which section" the whole
 * question, and the reason each one gets its own case here rather than one test over the
 * container.
 */
describe('Settings — a section that was never touched is not dirty', () => {
  let dirty: SettingsDirtyService;

  beforeEach(() => {
    localStorage.clear();
    TestBed.configureTestingModule({
      providers: [
        provideZonelessChangeDetection(),
        provideHttpClient(),
        provideHttpClientTesting(),
        SettingsDirtyService,
        { provide: LUCIDE_ICONS, multi: true, useValue: new LucideIconProvider({ ...icons }) },
      ],
    });
    dirty = TestBed.inject(SettingsDirtyService);
  });

  it('events section: mounting reports nothing dirty', async () => {
    const f = TestBed.createComponent(EventsSectionComponent);
    await f.whenStable();
    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('retention section: mounting, then the GET landing, reports nothing dirty', async () => {
    const f = TestBed.createComponent(RetentionSectionComponent);
    await f.whenStable();

    // The load is what makes this interesting: `saved` and the eight field signals are set
    // from the same response, so any difference between them afterwards is the component's
    // own doing rather than the user's.
    const http = TestBed.inject(HttpTestingController);
    const req = http.expectOne(r => r.url.includes('retention'));
    req.flush({
      verboseDays: 3, debugDays: 5, informationDays: 14, warningDays: 30,
      errorDays: 60, fatalDays: 90, metricsDays: 30, tracesDays: 7,
    });
    await f.whenStable();

    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('users section: mounting, with both loads landing, reports nothing dirty', async () => {
    const f = TestBed.createComponent(UsersSectionComponent);
    await f.whenStable();

    const http = TestBed.inject(HttpTestingController);
    // Both list loads answer with content, because "dirty" here is about the CREATE forms and
    // an empty install cannot show a form being prefilled from a response.
    for (const req of http.match(() => true)) {
      req.flush(req.request.url.includes('domain')
        ? [{ id: 'd1', provider: 'google', domain: 'example.com', role: 'viewer', permissions: 15 }]
        : [{ id: 'u1', username: 'admin', role: 'admin', provider: 'local', permissions: 15 }]);
    }
    await f.whenStable();

    expect(dirty.dirtyTabsList()).toEqual([]);
  });

  it('api-keys section: mounting, with the load landing, reports nothing dirty', async () => {
    const f = TestBed.createComponent(ApiKeysSectionComponent);
    await f.whenStable();

    const http = TestBed.inject(HttpTestingController);
    for (const req of http.match(() => true)) {
      req.flush([{ id: 'k1', name: 'agent', description: '', permissions: 15, createdAt: '2026-01-01T00:00:00Z' }]);
    }
    await f.whenStable();

    expect(dirty.dirtyTabsList()).toEqual([]);
  });
});
