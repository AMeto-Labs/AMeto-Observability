import { describe, expect, it } from 'vitest';

import { McpProfile } from '../../../../../core/models/mcp.model';
import { ProfileForm, blankForm, rebaseForm, rebaseList, sameForm, sameMembers, toForm } from './profile-form';

const PROFILE: McpProfile = {
  id: 'p1', name: 'Payments team', description: 'Claude Code', keyPreview: '7a3f09c1…',
  toolsMode: 'builtInAndPublished', tools: ['logs_overview', 'logs_search'], services: ['payments-api'], maskingMode: 'standard',
  freeFilters: true, maxEvents: 200, maxWindowMinutes: 90, callsPerMinute: 30, publishedTools: ['t1', 't2'],
  createdBy: 'admin', createdAt: '2026-11-10T08:00:00Z', lastCallAt: null,
};

const form = (change: Partial<ProfileForm> = {}): ProfileForm => ({ ...toForm(PROFILE), ...change });

/**
 * The profile card holds a copy of its profile while the profile itself can change under it:
 * the card's tab stays mounted while a tool's editor grants and withdraws tools. These pin how
 * the copy is brought up to date without losing what was typed into it.
 */
describe('AI / MCP — a profile card and the profile it was filled from', () => {
  it('fills the card with a window the card can save', () => {
    expect(toForm(PROFILE)).toMatchObject({ window: '90 min', published: ['t1', 't2'], builtIn: ['logs_overview', 'logs_search'] });
    expect(blankForm(['logs_search'])).toMatchObject({ name: '', builtIn: ['logs_search'], published: [], window: '24 h' });
  });

  it('counts a list by its members: switched off and on again is not a change', () => {
    expect(sameMembers(['t1', 't2'], ['t2', 't1'])).toBe(true);
    expect(sameMembers(['t1'], ['t1', 't2'])).toBe(false);
    expect(sameMembers([], [])).toBe(true);
    expect(sameForm(form(), form({ published: ['t2', 't1'], builtIn: ['logs_search', 'logs_overview'] }))).toBe(true);
    expect(sameForm(form(), form({ published: ['t1'] }))).toBe(false);
    expect(sameForm(form(), form({ maxEvents: null }))).toBe(false);
  });

  it('does what the user did to a list again, on the list as it is now', () => {
    // Untouched: the list as it is now.
    expect(rebaseList(['t1', 't2'], ['t1', 't2'], ['t2'])).toEqual(['t2']);
    expect(rebaseList(['t1'], ['t1'], ['t1', 't3'])).toEqual(['t1', 't3']);
    // The user switched t3 on; somebody withdrew t1 meanwhile: both hold.
    expect(rebaseList(['t1', 't2', 't3'], ['t1', 't2'], ['t2'])).toEqual(['t2', 't3']);
    // The user switched t2 off; somebody granted t4 meanwhile: both hold.
    expect(rebaseList(['t1'], ['t1', 't2'], ['t1', 't2', 't4'])).toEqual(['t1', 't4']);
    // The user and the server agree: nothing twice.
    expect(rebaseList(['t1', 't3'], ['t1'], ['t1', 't3'])).toEqual(['t1', 't3']);
  });

  it('with nothing unsaved, the card becomes the profile as it is now', () => {
    const fresh = form({ published: ['t2'], name: 'Payments developers', maxEvents: 100 });
    expect(rebaseForm(form(), form(), fresh)).toEqual(fresh);
  });

  it('with unsaved input, keeps what was typed and follows the profile everywhere else', () => {
    const base = form();
    // The user renamed the profile and switched t3 on …
    const mine = form({ name: 'Payments developers', published: ['t1', 't2', 't3'] });
    // … while a tool's editor withdrew t1 from it, and somebody else changed its description.
    const fresh = form({ published: ['t2'], description: 'Cursor, too' });

    const rebased = rebaseForm(mine, base, fresh);
    expect(rebased.name).toBe('Payments developers');
    expect(rebased.description).toBe('Cursor, too');
    expect(rebased.published).toEqual(['t2', 't3']);
    // Measured against the profile as it is now, the unsaved part is what the user did — no more.
    expect(sameForm(rebased, fresh)).toBe(false);
    expect(sameForm({ ...rebased, name: fresh.name, published: ['t2'] }, fresh)).toBe(true);
  });

  it('keeps an emptied number field emptied while the profile moves under it', () => {
    const rebased = rebaseForm(form({ maxEvents: null }), form(), form({ maxEvents: 500, callsPerMinute: 60 }));
    expect(rebased.maxEvents).toBeNull();
    expect(rebased.callsPerMinute).toBe(60);
  });
});
