import { ChangeDetectionStrategy, Component, DestroyRef, computed, effect, inject, signal, untracked } from '@angular/core';
import { Location } from '@angular/common';
import { takeUntilDestroyed, toSignal } from '@angular/core/rxjs-interop';
import { ActivatedRoute, NavigationStart, ParamMap, Router } from '@angular/router';
import { LucideAngularModule } from 'lucide-angular';

import { ModalComponent } from '../../../../shared/components/ui';
import { copyToClipboard } from '../../../../shared/utils/clipboard';
import { SettingsDirtyService } from '../../settings-dirty.service';
import { MCP_TABS, McpSectionStore, McpTabId, RevealedKey } from './mcp.store';
import { ConnectionTabComponent } from './connection-tab/connection-tab';
import { ProfilesTabComponent } from './profiles-tab/profiles-tab';
import { MaskingTabComponent } from './masking-tab/masking-tab';
import { ToolsTabComponent } from './tools-tab/tools-tab';
import { ToolEditorComponent } from './tool-editor/tool-editor';
import { ActivityTabComponent } from './activity-tab/activity-tab';

const isTab = (value: string | null): value is McpTabId => MCP_TABS.some(t => t.id === value);

/** A place in the section: a tab, and on the Tools tab the tool in the editor — its id, `new`, or null for the list. */
interface Place {
  tab: McpTabId;
  tool: string | null;
}

const placeOf = (params: ParamMap): Place => {
  const requested = params.get('mcp');
  const tab = isTab(requested) ? requested : 'connection';
  return { tab, tool: tab === 'tools' ? params.get('tool') || null : null };
};

const samePlace = (a: Place, b: Place): boolean => a.tab === b.tab && a.tool === b.tool;

/**
 * How long Done takes nothing down after it has taken a key down. The next key comes up in the
 * same dialog, its Done in the same place and still focused: without the pause, the second click
 * of a double click or a repeated Enter would take a key down that was never seen.
 */
export const DONE_PAUSE_MS = 700;

/**
 * How long after a key has come up a click the browser counts as the second or a later one of a
 * run is taken for the rest of a double click, and dropped. A system can be set to count clicks
 * 900 ms apart as one run, which is longer than the pause. Past this a click is somebody
 * clicking, however the browser counts it: every click made inside the system's interval
 * lengthens the run, and a rule with no end to it would drop them all, without a word.
 */
export const DOUBLE_CLICK_MS = 1000;

/**
 * Where the browser's current history entry stands among the others, where the browser says:
 * the Navigation API's index. Null without that API.
 */
const historyIndex = (): number | null =>
  (globalThis as { navigation?: { currentEntry?: { index: number } | null } }).navigation?.currentEntry?.index ?? null;

/**
 * Settings → AI / MCP: the Model Context Protocol endpoint for AI assistants — where it is and
 * how a client connects, the profiles and their keys, what is hidden from an answer, the tools
 * an assistant is offered, and the record of its calls (docs/MCP.md).
 *
 * The tab and the tool being edited live in the URL (`?mcp=tools&tool=<id>`), next to the
 * settings page's own `?tab=`: a refresh, a shared link and the back button all land where the
 * reader was.
 */
@Component({
  selector: 'app-mcp-section',
  imports: [
    LucideAngularModule, ModalComponent,
    ConnectionTabComponent, ProfilesTabComponent, MaskingTabComponent,
    ToolsTabComponent, ToolEditorComponent, ActivityTabComponent,
  ],
  providers: [McpSectionStore],
  templateUrl: './mcp-section.html',
  styleUrl: './mcp-section.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class McpSectionComponent {
  readonly store = inject(McpSectionStore);
  private readonly route = inject(ActivatedRoute);
  private readonly router = inject(Router);
  private readonly location = inject(Location);
  private readonly dirtyService = inject(SettingsDirtyService);

  readonly tabs = MCP_TABS;

  private readonly params = toSignal(this.route.queryParamMap, { initialValue: this.route.snapshot.queryParamMap });

  /** Where the URL points. */
  private readonly requested = computed(() => placeOf(this.params()), { equal: samePlace });

  /**
   * Where the section is. It goes where the URL points — with one exception: out of an editor
   * that holds unsaved input it goes only after the question. The editor is drawn from this,
   * not from the URL, because the URL has already changed by the time anyone can be asked: the
   * browser's Back and Forward do not wait for an answer.
   */
  private readonly place = signal<Place>(placeOf(this.route.snapshot.queryParamMap));

  /** The history entry of the place, by its index (see historyIndex); null where that is not known. */
  private placeEntry = historyIndex();

  /** True while the Router follows the browser's Back or Forward rather than a move of the page's own. */
  private traversing = false;

  readonly activeTab = computed<McpTabId>(() => this.place().tab);

  /** The tool open in the editor: its id, `new`, or null while the list is shown. */
  readonly editing = computed<string | null>(() => this.place().tool);

  /** The tabs opened so far; each is created on its first visit. */
  readonly visited = signal<ReadonlySet<McpTabId>>(new Set<McpTabId>());

  /** Counts "Create a profile key" clicks on the Connection tab; the Profiles tab opens a blank profile on each. */
  readonly createRequest = signal(0);

  /**
   * The key on screen: the first of those that arrived and have not been taken down. Shown
   * here, not on the Profiles tab that asked for it: by the time a key arrives that tab may be
   * hidden behind another, and a dialog inside a hidden tab is not seen at all.
   */
  readonly revealed = computed<RevealedKey | null>(() => this.store.keys()[0] ?? null);
  /** Named as the list names the profile now; a profile gone from the list, as it was named when its key arrived. */
  readonly keyTitle = computed(() => {
    const r = this.revealed();
    if (!r) return '';
    return `Key of “${this.store.profiles().find(p => p.id === r.profileId)?.name ?? r.profile}”`;
  });
  /** Said under the key while others wait behind it: each is shown once, and none should come as a surprise. */
  readonly keysBehind = computed(() => {
    const behind = this.store.keys().length - 1;
    if (behind < 1) return '';
    return behind === 1
      ? 'Another key is waiting: it is shown after this one.'
      : `${behind} more keys are waiting: they are shown one at a time after this one.`;
  });
  /** The key whose Copy button says it was copied, for a moment; a key shown after it was not. */
  readonly copied = signal<string | null>(null);
  /** True for a moment after Done took a key down (see DONE_PAUSE_MS): the key shown meanwhile cannot be taken down yet. */
  readonly doneHeld = signal(false);
  private donePause: ReturnType<typeof setTimeout> | undefined;
  /** When the key on screen came up (see DOUBLE_CLICK_MS). */
  private shownAt = 0;

  constructor() {
    inject(DestroyRef).onDestroy(() => clearTimeout(this.donePause));
    effect(() => {
      if (this.revealed()) this.shownAt = Date.now();
    });
    this.router.events.pipe(takeUntilDestroyed()).subscribe(e => {
      if (e instanceof NavigationStart) this.traversing = e.navigationTrigger === 'popstate';
    });
    effect(() => this.dirtyService.mark('ai-mcp', this.store.anyDirty()));
    effect(() => {
      const requested = this.requested();
      untracked(() => this.follow(requested));
    });
    effect(() => {
      const tab = this.activeTab();
      untracked(() => this.visited.update(seen => (seen.has(tab) ? seen : new Set(seen).add(tab))));
    });
    void this.store.load();
  }

  select(tab: McpTabId): void {
    if (tab === this.activeTab() && this.editing() === null) return;
    if (!this.mayLeaveEditor()) return;
    this.go({ mcp: tab, tool: null });
  }

  /** Opens the editor on a tool (`new` for a blank one). `afterSave` skips the question: there is nothing left to lose. */
  openTool(id: string, afterSave = false): void {
    if (!afterSave && !this.mayLeaveEditor()) return;
    this.go({ mcp: 'tools', tool: id });
  }

  closeEditor(): void {
    this.go({ mcp: 'tools', tool: null });
  }

  newProfile(): void {
    this.createRequest.update(n => n + 1);
    this.go({ mcp: 'profiles', tool: null });
  }

  /** Copy: the key this button was drawn beside, as Done takes down the one it was drawn for. */
  async copyKey(shown: RevealedKey): Promise<void> {
    const { key } = shown;
    await copyToClipboard(key);
    this.copied.set(key);
    setTimeout(() => this.copied.update(k => (k === key ? null : k)), 1500);
  }

  /**
   * Done: takes down `key`, the key this Done was drawn for — not whatever is first by the time
   * the click is handled. Then Done takes nothing down for a moment, whether the next key comes
   * up in its place at once or arrives just after the dialog closed; a key shown with nothing
   * taken down just before it is not held up.
   *
   * A click that is the second or a later one of a run — a double click, however slow the
   * system lets one be — takes nothing down either while the key has only just come up: the
   * pause is 700 ms, and a double click can be set to 900. That covers a key that comes up
   * under a double click begun elsewhere as well. The pause stays for whatever does not count
   * its clicks.
   */
  done(key: RevealedKey, click?: Event): void {
    if (click instanceof MouseEvent && click.detail > 1 && Date.now() - this.shownAt < DOUBLE_CLICK_MS) return;
    if (this.doneHeld() || this.revealed() !== key) return;
    this.store.acknowledgeKey(key);
    this.doneHeld.set(true);
    clearTimeout(this.donePause);
    this.donePause = setTimeout(() => this.doneHeld.set(false), DONE_PAUSE_MS);
  }

  /**
   * Enter held down on Done repeats, and each repeat is a click: past the pause the next key
   * would be taken down unseen. A repeat is not a press; it activates nothing.
   */
  holdRepeat(key: KeyboardEvent): void {
    if (key.repeat && (key.key === 'Enter' || key.key === ' ')) key.preventDefault();
  }

  /** Leaving the editor drops what it holds; ask once when that is something. */
  private mayLeaveEditor(): boolean {
    if (this.editing() === null || !this.store.dirty().tool) return true;
    if (!confirm('This tool has unsaved changes. Discard them?')) return false;
    this.store.setDirty('tool', false);
    return true;
  }

  /**
   * Goes where the URL now points. A move made inside the section has asked its question
   * before it touched the URL, and cleared the flag; what arrives here with the flag still up
   * is Back, Forward or an address typed by hand — out of an editor with unsaved input. "No"
   * keeps the editor, and its address with it (see `stay`).
   */
  private follow(requested: Place): void {
    const here = this.place();
    if (samePlace(requested, here)) return;
    if (here.tool !== null && this.store.dirty().tool) {
      if (!confirm('This tool has unsaved changes. Discard them?')) {
        this.stay(here);
        return;
      }
      this.store.setDirty('tool', false);
    }
    this.place.set(requested);
    this.placeEntry = historyIndex();
  }

  /**
   * "No" to leaving the editor: the address goes back to the editor's. After Back or Forward
   * that is a step back through the history to the editor's own entry, so the entry Back went
   * to is still there for the next Back. Where the browser does not say where its entries
   * stand, the editor's address takes the place of the one just entered instead — that entry
   * is lost, but the address is right.
   */
  private stay(here: Place): void {
    const now = historyIndex();
    if (this.traversing && this.placeEntry !== null && now !== null && now !== this.placeEntry) {
      this.location.historyGo(this.placeEntry - now);
      return;
    }
    this.go({ mcp: here.tab, tool: here.tool }, true);
  }

  private go(queryParams: Record<string, string | null>, replaceUrl = false): void {
    void this.router.navigate([], { relativeTo: this.route, queryParams, queryParamsHandling: 'merge', replaceUrl });
  }
}
