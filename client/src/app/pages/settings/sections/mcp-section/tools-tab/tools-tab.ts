import { ChangeDetectionStrategy, Component, computed, inject, output, signal } from '@angular/core';
import { toObservable, toSignal } from '@angular/core/rxjs-interop';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';
import { catchError, map, of, switchMap } from 'rxjs';

import { ModalComponent, SectionComponent } from '../../../../../shared/components/ui';
import { McpApiService, mcpError } from '../../../../../core/services/mcp-api.service';
import { McpOfferedTool, McpPublishedTool } from '../../../../../core/models/mcp.model';
import { McpSectionStore } from '../mcp.store';
import { fmtCount, signatureOf } from '../mcp-format';

/**
 * Tools: the five built in, with a switch each for the whole server; the published ones, which
 * are saved queries an assistant calls by name; and, on the right, the list one profile's key
 * is actually offered — asked of the server, not worked out here.
 */
@Component({
  selector: 'app-mcp-tools-tab',
  imports: [FormsModule, LucideAngularModule, SectionComponent, ModalComponent],
  templateUrl: './tools-tab.html',
  styleUrl: './tools-tab.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ToolsTabComponent {
  readonly store = inject(McpSectionStore);
  private readonly api = inject(McpApiService);

  /** Open the editor: a tool's id, or `new`. */
  readonly edit = output<string>();

  // ── Built-in ────────────────────────────────────────────────────────────────

  readonly builtIn = computed(() => {
    const usage = this.store.usage();
    return (this.store.overview()?.tools ?? []).map(t => ({ tool: t, calls: fmtCount(usage[t.name] ?? 0) }));
  });

  readonly switching = signal<string | null>(null);
  readonly switchError = signal<string | null>(null);

  async toggleBuiltIn(name: string, enabled: boolean): Promise<void> {
    this.switching.set(name);
    this.switchError.set(null);
    try {
      await this.store.setBuiltInTool(name, enabled);
    } catch (e) {
      this.switchError.set(mcpError(e, 'The tool could not be switched.').error);
    } finally {
      this.switching.set(null);
    }
  }

  // ── Published ───────────────────────────────────────────────────────────────

  readonly published = computed(() => {
    const usage = this.store.usage();
    return this.store.tools().map(t => ({
      tool: t,
      signature: signatureOf(t.inputSchema, true, ' · ') || '—',
      profiles: t.profiles.length ? String(t.profiles.length) : '—',
      calls: t.published ? fmtCount(usage[t.name] ?? 0) : '—',
    }));
  });

  readonly deleting = signal<McpPublishedTool | null>(null);
  readonly working = signal(false);
  readonly deleteError = signal<string | null>(null);

  askDelete(tool: McpPublishedTool): void {
    this.deleteError.set(null);
    this.deleting.set(tool);
  }

  async remove(): Promise<void> {
    const tool = this.deleting();
    if (!tool) return;
    this.working.set(true);
    try {
      await this.store.deleteTool(tool.id);
      this.deleting.set(null);
    } catch (e) {
      this.deleteError.set(mcpError(e, 'The tool could not be deleted.').error);
    } finally {
      this.working.set(false);
    }
  }

  // ── The list one profile gets ───────────────────────────────────────────────

  /** The profile picked in the panel; the first one until another is picked. */
  private readonly picked = signal<string | null>(null);
  readonly listProfile = computed(() => {
    const profiles = this.store.profiles();
    return profiles.find(p => p.id === this.picked()) ?? profiles[0] ?? null;
  });

  readonly listNote = computed(() => {
    const p = this.listProfile();
    if (!p) return '';
    return `${p.toolsMode === 'publishedOnly' ? 'Published only' : 'Built-in and published'} · ${p.maskingMode === 'strict' ? 'Strict' : 'Standard'} masking`;
  });

  /** Asked again whenever the profile, the tools or the server's switches change. Null: the list could not be read. */
  readonly offered = toSignal(
    toObservable(computed(() => ({
      id: this.listProfile()?.id ?? null, profiles: this.store.profiles(), tools: this.store.tools(), overview: this.store.overview(),
    }))).pipe(
      switchMap(({ id }) => (id
        ? this.api.getOfferedTools(id).pipe(map(r => r.tools as McpOfferedTool[] | null), catchError(() => of(null)))
        : of([] as McpOfferedTool[]))),
    ),
    { initialValue: [] as McpOfferedTool[] | null },
  );

  readonly offeredRows = computed(() =>
    (this.offered() ?? []).map(t => ({ tool: t, signature: signatureOf(t.inputSchema, !t.builtIn) || 'no arguments' })));

  readonly offeredNote = computed(() => {
    const n = this.offered()?.length ?? 0;
    const p = this.listProfile();
    return `${n} ${n === 1 ? 'tool' : 'tools'}.` + (p?.toolsMode === 'publishedOnly' ? ' Built-in tools are not offered to this profile.' : '');
  });

  pickProfile(id: string): void {
    this.picked.set(id);
  }
}
