import { computed, inject } from '@angular/core';
import { patchState, signalStore, withComputed, withMethods, withState } from '@ngrx/signals';
import { catchError, firstValueFrom, forkJoin, of } from 'rxjs';

import { ApiService } from '../../../../core/services/api.service';
import { McpApiService, McpSettingsPatch, mcpError } from '../../../../core/services/mcp-api.service';
import {
  McpOverview, McpProfile, McpProfileInput, McpPublishedTool, McpRule, McpRuleInput, McpSummary, McpToolInput,
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

/** The parts of the section that can hold unsaved input. */
export type McpDirtyPart = 'limits' | 'profile' | 'tool';

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
  /** Calls of the last seven days, by tool name. */
  usage: Record<string, number>;
  /** The last 24 hours added up; null until it has been read, or when it cannot be. */
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
  usage: {},
  summary: null,
  services: [],
  dirty: { limits: false, profile: false, tool: false },
};

/**
 * What Settings → AI / MCP knows, shared by its tabs: the section's settings, the profiles,
 * the masking rules and the tools, with the figures read from the record of calls. Provided by
 * the section component, so it lives exactly as long as the section is mounted.
 *
 * A change is a method here: it calls the API, and puts what the server answered — never what
 * the form held — into the state. A refused change rejects with the server's own sentence
 * (see {@link mcpError}), and the state stays as it was.
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
  withMethods((store, api = inject(McpApiService), logs = inject(ApiService)) => {
    /** The figures: best effort, each on its own — a busy server must not empty the section. */
    const loadFigures = (): void => {
      api.getSummary().pipe(catchError(() => of(null))).subscribe(summary => patchState(store, { summary }));
      api.getUsage().pipe(catchError(() => of(null))).subscribe(usage => patchState(store, { usage: usage?.calls ?? {} }));
    };

    /** Profiles and tools name each other (a grant is one fact with two ends): read both again. */
    const reloadProfilesAndTools = async (): Promise<void> => {
      const [profiles, tools] = await firstValueFrom(forkJoin([api.getProfiles(), api.getTools()]));
      patchState(store, { profiles, tools: tools.published });
    };

    return {
      async load(): Promise<void> {
        patchState(store, { loading: true, loadError: null });
        try {
          const [overview, profiles, rules, tools] = await firstValueFrom(
            forkJoin([api.getOverview(), api.getProfiles(), api.getRules(), api.getTools()]));
          patchState(store, { overview, profiles, rules, tools: tools.published, loading: false, loaded: true });
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

      setDirty(part: McpDirtyPart, dirty: boolean): void {
        if (store.dirty()[part] === dirty) return;
        patchState(store, { dirty: { ...store.dirty(), [part]: dirty } });
      },

      // ── Settings ──────────────────────────────────────────────────────────

      async updateSettings(patch: McpSettingsPatch): Promise<void> {
        patchState(store, { overview: await firstValueFrom(api.updateSettings(patch)) });
      },

      /** Switches one detector; the others stay as they are. */
      async setDetector(id: string, on: boolean): Promise<void> {
        const detectors = (store.overview()?.detectors ?? [])
          .filter(d => (d.id === id ? on : d.on)).map(d => d.id);
        patchState(store, { overview: await firstValueFrom(api.updateSettings({ detectors })) });
      },

      /** Switches one built-in tool for the whole server. */
      async setBuiltInTool(name: string, enabled: boolean): Promise<void> {
        const disabledTools = (store.overview()?.tools ?? [])
          .filter(t => (t.name === name ? !enabled : !t.enabled)).map(t => t.name);
        patchState(store, { overview: await firstValueFrom(api.updateSettings({ disabledTools })) });
      },

      async rotateSecret(): Promise<void> {
        const { secretCreatedAt } = await firstValueFrom(api.rotateSecret());
        const overview = store.overview();
        if (overview) patchState(store, { overview: { ...overview, secretCreatedAt } });
      },

      // ── Profiles ──────────────────────────────────────────────────────────

      /** Answers the new profile with its key: the only time the key exists outside the client's hands. */
      async createProfile(input: McpProfileInput): Promise<{ profile: McpProfile; key: string }> {
        const created = await firstValueFrom(api.createProfile(input));
        await reloadProfilesAndTools();
        return created;
      },

      async updateProfile(id: string, input: McpProfileInput): Promise<void> {
        await firstValueFrom(api.updateProfile(id, input));
        await reloadProfilesAndTools();
      },

      async deleteProfile(id: string): Promise<void> {
        await firstValueFrom(api.deleteProfile(id));
        await reloadProfilesAndTools();
      },

      /** Answers the new key; the old one has stopped working by then. */
      async rotateKey(id: string): Promise<string> {
        const { key } = await firstValueFrom(api.rotateProfileKey(id));
        patchState(store, { profiles: await firstValueFrom(api.getProfiles()) });
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
        await reloadProfilesAndTools();
        return saved;
      },

      async deleteTool(id: string): Promise<void> {
        await firstValueFrom(api.deleteTool(id));
        await reloadProfilesAndTools();
      },
    };
  }),
);

export type McpSectionStoreType = InstanceType<typeof McpSectionStore>;
