import { ChangeDetectionStrategy, Component, computed, effect, inject, signal, untracked } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';
import { ActivatedRoute, Router } from '@angular/router';

import { SettingsDirtyService } from '../../settings-dirty.service';
import { MCP_TABS, McpSectionStore, McpTabId } from './mcp.store';
import { ConnectionTabComponent } from './connection-tab/connection-tab';
import { ProfilesTabComponent } from './profiles-tab/profiles-tab';
import { MaskingTabComponent } from './masking-tab/masking-tab';
import { ToolsTabComponent } from './tools-tab/tools-tab';
import { ToolEditorComponent } from './tool-editor/tool-editor';
import { ActivityTabComponent } from './activity-tab/activity-tab';

const isTab = (value: string | null): value is McpTabId => MCP_TABS.some(t => t.id === value);

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
  private readonly dirtyService = inject(SettingsDirtyService);

  readonly tabs = MCP_TABS;

  private readonly params = toSignal(this.route.queryParamMap, { initialValue: this.route.snapshot.queryParamMap });

  readonly activeTab = computed<McpTabId>(() => {
    const requested = this.params().get('mcp');
    return isTab(requested) ? requested : 'connection';
  });

  /** The tool open in the editor: its id, `new`, or null while the list is shown. */
  readonly editing = computed<string | null>(() =>
    this.activeTab() === 'tools' ? this.params().get('tool') || null : null);

  /** The tabs opened so far; each is created on its first visit. */
  readonly visited = signal<ReadonlySet<McpTabId>>(new Set<McpTabId>());

  /** Counts "Create a profile key" clicks on the Connection tab; the Profiles tab opens a blank profile on each. */
  readonly createRequest = signal(0);

  constructor() {
    effect(() => this.dirtyService.mark('ai-mcp', this.store.anyDirty()));
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

  /** Leaving the editor drops what it holds; ask once when that is something. */
  private mayLeaveEditor(): boolean {
    if (this.editing() === null || !this.store.dirty().tool) return true;
    if (!confirm('This tool has unsaved changes. Discard them?')) return false;
    this.store.setDirty('tool', false);
    return true;
  }

  private go(queryParams: Record<string, string | null>): void {
    void this.router.navigate([], { relativeTo: this.route, queryParams, queryParamsHandling: 'merge' });
  }
}
