import { DestroyRef, computed, inject } from '@angular/core';
import { patchState, signalStore, withComputed, withMethods, withState } from '@ngrx/signals';
import { Subscription, TimeoutError, catchError, firstValueFrom, forkJoin, of, timeout } from 'rxjs';

import { ApiService } from '../../../../core/services/api.service';
import { McpApiService, McpSettingsPatch, mcpError } from '../../../../core/services/mcp-api.service';
import {
  McpOverview, McpProfile, McpProfileInput, McpProfilePatch, McpPublishedTool, McpRule, McpRuleInput, McpSummary,
  McpToolInput,
} from '../../../../core/models/mcp.model';

/** The section's tabs, in the order they are shown. */
export const MCP_TABS = [
  { id: 'connection', label: 'Connection' },
  { id: 'profiles',   label: 'Profiles' },
  { id: 'masking',    label: 'Masking' },
  { id: 'tools',      label: 'Tools' },
  { id: 'activity',   label: 'Activity' },
] as const;

export type McpTabId = (typeof MCP_TABS)[number]['id'];

/**
 * The parts of the section that hold something a reload would lose: unsaved input, and a
 * profile's key while it is on its way or on screen — it is shown once, and the key it
 * replaced has stopped working.
 */
export type McpDirtyPart = 'limits' | 'profile' | 'tool' | 'key';

/**
 * How long a change of the section's settings is waited for. The changes go one at a time, so
 * one that is never answered would hold every switch behind it for good.
 */
export const SETTINGS_CHANGE_TIMEOUT_MS = 30_000;

interface McpSectionState {
  loading: boolean;
  loaded: boolean;
  /** Why the section could not be read; null once it has been. */
  loadError: string | null;
  overview: McpOverview | null;
  profiles: McpProfile[];
  rules: McpRule[];
  /** Saved tools, drafts included. */
  tools: McpPublishedTool[];
  /**
   * True when a change went through and the profiles and tools could not be read again after
   * it: the change is in the lists, anything else that moved on the server meanwhile is not.
   */
  stale: boolean;
  /** Calls of the last seven days, by tool name. */
  usage: Record<string, number>;
  /** The last 24 hours added up; null until it has been read. */
  summary: McpSummary | null;
  /** Service names seen lately, for the fields that take one. */
  services: string[];
  dirty: Record<McpDirtyPart, boolean>;
}

const initialState: McpSectionState = {
  loading: false,
  loaded: false,
  loadError: null,
  overview: null,
  profiles: [],
  rules: [],
  tools: [],
  stale: false,
  usage: {},
  summary: null,
  services: [],
  dirty: { limits: false, profile: false, tool: false, key: false },
};

/** The two lists that name each other. */
type Lists = Pick<McpSectionState, 'profiles' | 'tools'>;

/** `list` with `id` in it or out of it, as `member` says; the list itself when it already is so. */
const withMember = (list: string[], id: string, member: boolean): string[] =>
  list.includes(id) === member ? list : member ? [...list, id] : list.filter(x => x !== id);

/*
 * A grant is one fact with two ends — a profile's `publishedTools` and a tool's `profiles` — and
 * the answer to a change carries one of them. The four functions below put the other end in
 * step, so the lists are whole from the answer alone: before they are read again, and when that
 * read fails.
 */

const withProfile = ({ profiles, tools }: Lists, saved: McpProfile): Lists => ({
  profiles: profiles.some(p => p.id === saved.id) ? profiles.map(p => (p.id === saved.id ? saved : p)) : [...profiles, saved],
  tools: tools.map(t => {
    const granted = withMember(t.profiles, saved.id, saved.publishedTools.includes(t.id));
    return granted === t.profiles ? t : { ...t, profiles: granted };
  }),
});

const withoutProfile = ({ profiles, tools }: Lists, id: string): Lists => ({
  profiles: profiles.filter(p => p.id !== id),
  tools: tools.map(t => (t.profiles.includes(id) ? { ...t, profiles: withMember(t.profiles, id, false) } : t)),
});

const withTool = ({ profiles, tools }: Lists, saved: McpPublishedTool): Lists => ({
  tools: tools.some(t => t.id === saved.id) ? tools.map(t => (t.id === saved.id ? saved : t)) : [...tools, saved],
  profiles: profiles.map(p => {
    const granted = withMember(p.publishedTools, saved.id, saved.profiles.includes(p.id));
    return granted === p.publishedTools ? p : { ...p, publishedTools: granted };
  }),
});

const withoutTool = ({ profiles, tools }: Lists, id: string): Lists => ({
  tools: tools.filter(t => t.id !== id),
  profiles: profiles.map(p => (p.publishedTools.includes(id) ? { ...p, publishedTools: withMember(p.publishedTools, id, false) } : p)),
});

const same = (a: unknown, b: unknown): boolean => JSON.stringify(a) === JSON.stringify(b);

/**
 * What Settings → AI / MCP knows, shared by its tabs: the section's settings, the profiles,
 * the masking rules and the tools, with the figures read from the record of calls. Provided by
 * the section component, so it lives exactly as long as the section is mounted.
 *
 * A change is a method here: it calls the API, and puts what the server answered — never what
 * the form held — into the state. A refused change rejects with the `HttpErrorResponse` itself
 * and leaves the state as it was; the caller turns it into the server's own sentence, and the
 * field that sentence is about, with {@link mcpError}.
 *
 * A change that went through has gone through: what follows it here — reading the lists again —
 * is best effort, and its failure is never the change's (see `stale`).
 */
export const McpSectionStore = signalStore(
  withState(initialState),
  withComputed(store => ({
    /** Detector id → the name Settings lists it under. */
    detectorNames: computed<Record<string, string>>(() =>
      Object.fromEntries((store.overview()?.detectors ?? []).map(d => [d.id, d.name]))),
    /** True when any part of the section holds input that has not been saved. */
    anyDirty: computed(() => Object.values(store.dirty()).some(Boolean)),
    /** Saved tools that are published: the ones a granted profile is actually offered. */
    publishedTools: computed(() => store.tools().filter(t => t.published)),
  })),
  withMethods((store, api = inject(McpApiService), logs = inject(ApiService), destroyRef = inject(DestroyRef)) => {
    let figures: Subscription | null = null;
    let listsRead: Subscription | null = null;
    destroyRef.onDestroy(() => {
      figures?.unsubscribe();
      listsRead?.unsubscribe();
    });

    /**
     * The figures: best effort, each on its own — a busy server must not empty the section. A
     * read that fails leaves the figures it would have replaced, and a read asked for while
     * another is on its way takes its place.
     */
    const loadFigures = (): void => {
      figures?.unsubscribe();
      figures = new Subscription();
      figures.add(api.getSummary().subscribe({ next: summary => patchState(store, { summary }), error: () => undefined }));
      figures.add(api.getUsage().subscribe({ next: usage => patchState(store, { usage: usage.calls ?? {} }), error: () => undefined }));
    };

    const lists = (): Lists => ({ profiles: store.profiles(), tools: store.tools() });

    /**
     * Reads the profiles and the tools again, in the background. The change that asked for it
     * is in the state already, from its own answer; this brings what an answer does not carry —
     * a rotated key's preview, what somebody else changed meanwhile. A later read replaces an
     * earlier one still on its way, so an older picture never lands on a newer one.
     */
    const readListsAgain = (): void => {
      listsRead?.unsubscribe();
      listsRead = forkJoin([api.getProfiles(), api.getTools()]).subscribe({
        next: ([profiles, tools]) => patchState(store, {
          // A list that came back as it stands is left alone: nothing that watches it has to move.
          ...(same(profiles, store.profiles()) ? {} : { profiles }),
          ...(same(tools.published, store.tools()) ? {} : { tools: tools.published }),
          stale: false,
        }),
        error: () => patchState(store, { stale: true }),
      });
    };

    /**
     * The detector and tool switches travel as whole lists, and every answer to
     * `PUT /settings` is the whole section. So these changes go one at a time, and each is
     * worked out when its turn comes: from the state the change before it left, not from the
     * state at the click. Two quick clicks would otherwise send two lists, each of which
     * undoes the other's switch.
     *
     * A change that is not answered in time is given up on, so the ones behind it can go. It
     * may still have been made, though, so the section is read again before the next one is
     * worked out — from the server's settings, not from the state before the change.
     */
    let settingsChange: Promise<unknown> = Promise.resolve();
    const changeSettings = (patch: () => McpSettingsPatch): Promise<void> => {
      const change = settingsChange.then(async () => {
        try {
          const overview = await firstValueFrom(api.updateSettings(patch()).pipe(timeout(SETTINGS_CHANGE_TIMEOUT_MS)));
          patchState(store, { overview });
        } catch (e) {
          if (e instanceof TimeoutError) {
            await firstValueFrom(api.getOverview().pipe(timeout(SETTINGS_CHANGE_TIMEOUT_MS)))
              .then(overview => patchState(store, { overview }), () => undefined);
          }
          throw e;
        }
      });
      settingsChange = change.catch(() => undefined);
      return change;
    };

    return {
      async load(): Promise<void> {
        patchState(store, { loading: true, loadError: null });
        try {
          const [overview, profiles, rules, tools] = await firstValueFrom(
            forkJoin([api.getOverview(), api.getProfiles(), api.getRules(), api.getTools()]));
          patchState(store, { overview, profiles, rules, tools: tools.published, loading: false, loaded: true, stale: false });
        } catch (e) {
          patchState(store, { loading: false, loadError: mcpError(e, 'The AI / MCP settings could not be loaded.').error });
          return;
        }
        loadFigures();
        logs.getServiceNames().pipe(catchError(() => of([] as string[])))
          .subscribe(services => patchState(store, { services }));
      },

      refreshFigures(): void {
        loadFigures();
      },

      /** Reads the profiles and the tools again: what "Read again" does when `stale` is shown. */
      refreshLists(): void {
        readListsAgain();
      },

      setDirty(part: McpDirtyPart, dirty: boolean): void {
        if (store.dirty()[part] === dirty) return;
        patchState(store, { dirty: { ...store.dirty(), [part]: dirty } });
      },

      // ── Settings ──────────────────────────────────────────────────────────

      updateSettings(patch: McpSettingsPatch): Promise<void> {
        return changeSettings(() => patch);
      },

      /** Switches one detector; the others stay as they are. */
      setDetector(id: string, on: boolean): Promise<void> {
        return changeSettings(() => ({
          detectors: (store.overview()?.detectors ?? []).filter(d => (d.id === id ? on : d.on)).map(d => d.id),
        }));
      },

      /** Switches one built-in tool for the whole server. */
      setBuiltInTool(name: string, enabled: boolean): Promise<void> {
        return changeSettings(() => ({
          disabledTools: (store.overview()?.tools ?? []).filter(t => (t.name === name ? !enabled : !t.enabled)).map(t => t.name),
        }));
      },

      async rotateSecret(): Promise<void> {
        const { secretCreatedAt } = await firstValueFrom(api.rotateSecret());
        const overview = store.overview();
        if (overview) patchState(store, { overview: { ...overview, secretCreatedAt } });
      },

      // ── Profiles ──────────────────────────────────────────────────────────

      /**
       * Answers the new profile with its key: the only time the key exists outside the client's
       * hands. That is why nothing stands between the server's answer and this one — the lists
       * are read again afterwards, and a key is never lost to that read.
       */
      async createProfile(input: McpProfileInput): Promise<{ profile: McpProfile; key: string }> {
        const created = await firstValueFrom(api.createProfile(input));
        patchState(store, withProfile(lists(), created.profile));
        readListsAgain();
        return created;
      },

      /** Changes the fields `input` names, and answers the profile as the server kept it. */
      async updateProfile(id: string, input: McpProfilePatch): Promise<McpProfile> {
        const saved = await firstValueFrom(api.updateProfile(id, input));
        patchState(store, withProfile(lists(), saved));
        readListsAgain();
        return saved;
      },

      async deleteProfile(id: string): Promise<void> {
        await firstValueFrom(api.deleteProfile(id));
        patchState(store, withoutProfile(lists(), id));
        readListsAgain();
      },

      /**
       * Answers the new key; the old one has stopped working by then, so the new one is handed
       * over at once. Its preview in the list is the one thing only a read can bring.
       */
      async rotateKey(id: string): Promise<string> {
        const { key } = await firstValueFrom(api.rotateProfileKey(id));
        readListsAgain();
        return key;
      },

      // ── Field rules ───────────────────────────────────────────────────────

      async saveRule(id: string | null, rule: McpRuleInput): Promise<void> {
        const saved: McpRule = await firstValueFrom(id ? api.updateRule(id, rule) : api.createRule(rule));
        patchState(store, {
          rules: id ? store.rules().map(r => (r.id === id ? saved : r)) : [...store.rules(), saved],
        });
      },

      async deleteRule(id: string): Promise<void> {
        await firstValueFrom(api.deleteRule(id));
        patchState(store, { rules: store.rules().filter(r => r.id !== id) });
      },

      // ── Tools ─────────────────────────────────────────────────────────────

      async saveTool(id: string | null, tool: McpToolInput): Promise<McpPublishedTool> {
        const saved = await firstValueFrom(id ? api.updateTool(id, tool) : api.createTool(tool));
        patchState(store, withTool(lists(), saved));
        readListsAgain();
        return saved;
      },

      async deleteTool(id: string): Promise<void> {
        await firstValueFrom(api.deleteTool(id));
        patchState(store, withoutTool(lists(), id));
        readListsAgain();
      },
    };
  }),
);
