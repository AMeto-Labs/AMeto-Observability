import {
  ChangeDetectionStrategy, Component, DestroyRef, computed, effect, inject, input, linkedSignal, output, signal, untracked,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';

import { McpApiService, mcpError } from '../../../../../core/services/mcp-api.service';
import {
  McpParameterType, McpPublishedTool, McpToolInput, McpToolParameter, McpToolTestResult,
} from '../../../../../core/models/mcp.model';
import { McpSectionStore } from '../mcp.store';
import { fmtCount, fmtWindow, parseWindow, placeholderNames, placeholderSegments, tokenSegments } from '../mcp-format';

/** A parameter as its row holds it: the allowed values of an enum as one comma-separated text. */
interface ParameterForm {
  name: string;
  type: McpParameterType;
  required: boolean;
  description: string;
  values: string;
  default: string;
}

interface ToolForm {
  name: string;
  description: string;
  parameters: ParameterForm[];
  filter: string;
  fields: string[];
  window: string;
  maxEvents: number;
  order: 'newest' | 'oldest';
  profiles: string[];
}

const NEW = 'new';

const blank = (): ToolForm => ({
  name: '', description: '', parameters: [], filter: '', fields: [],
  window: '1 h', maxEvents: 200, order: 'newest', profiles: [],
});

const toForm = (tool: McpPublishedTool | null): ToolForm => (tool ? {
  name: tool.name,
  description: tool.description,
  parameters: tool.parameters.map(p => ({
    name: p.name, type: p.type, required: p.required, description: p.description,
    values: p.values.join(', '), default: p.default ?? '',
  })),
  filter: tool.filter,
  fields: [...tool.fields],
  window: fmtWindow(tool.defaultWindowMinutes),
  maxEvents: tool.maxEvents,
  order: tool.order,
  profiles: [...tool.profiles],
} : blank());

/**
 * The editor of a published tool: a saved query with parameters, which an assistant calls by
 * name. On the left the definition; on the right a test run that answers exactly as a chosen
 * profile would be answered — the same masking, the same limits — without saving anything.
 *
 * What may be saved is the server's to say (a name already taken, a placeholder nobody
 * declared, a filter that reads a hidden field in a way no call may): its sentence is shown
 * next to the field it names.
 */
@Component({
  selector: 'app-mcp-tool-editor',
  imports: [FormsModule, LucideAngularModule],
  templateUrl: './tool-editor.html',
  styleUrl: './tool-editor.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class ToolEditorComponent {
  readonly store = inject(McpSectionStore);
  private readonly api = inject(McpApiService);

  /** The tool's id, or `new`. */
  readonly toolId = input.required<string>();
  /** Back to the list. */
  readonly closed = output<void>();
  /** A save went through: the tool's id, which for a new tool is new too. */
  readonly saved = output<string>();

  readonly isNew = computed(() => this.toolId() === NEW);
  readonly tool = computed<McpPublishedTool | null>(() =>
    this.isNew() ? null : this.store.tools().find(t => t.id === this.toolId()) ?? null);
  /** True for an id that is no tool's: it was deleted, or the link is stale. */
  readonly missing = computed(() => !this.isNew() && this.tool() === null);
  readonly isPublished = computed(() => this.tool()?.published ?? false);

  /**
   * The form follows the tool that is OPEN — it is filled when the editor moves to another
   * tool, never because the list of tools was read again while this one was being edited.
   */
  readonly form = linkedSignal<string, ToolForm>({
    source: () => this.toolId(),
    computation: () => toForm(untracked(this.tool)),
  });
  private readonly baseline = linkedSignal<string, string>({
    source: () => this.toolId(),
    computation: () => JSON.stringify(toForm(untracked(this.tool))),
  });
  readonly dirty = computed(() => JSON.stringify(this.form()) !== this.baseline());

  readonly saving = signal(false);
  readonly error = signal<string | null>(null);
  readonly errorField = signal<string | null>(null);
  readonly fieldDraft = signal('');

  readonly types: readonly McpParameterType[] = ['string', 'number', 'boolean', 'enum'];

  readonly windowMinutes = computed(() => parseWindow(this.form().window));

  /** The filter in runs of text and `{parameter}` placeholders, for the layer drawn under the textarea. */
  readonly filterSegments = computed(() => placeholderSegments(this.form().filter));

  /** Placeholders nobody declared, and parameters no placeholder uses: both are refused on save. */
  readonly placeholderProblems = computed(() => {
    const used = placeholderNames(this.form().filter);
    const declared = this.form().parameters.map(p => p.name.trim()).filter(Boolean);
    const parts: string[] = [];
    const undeclared = used.filter(n => !declared.includes(n));
    const unused = declared.filter(n => !used.includes(n));
    if (undeclared.length) parts.push(`Not declared as a parameter: ${undeclared.map(n => `{${n}}`).join(', ')}.`);
    if (unused.length) parts.push(`Not used in the filter: ${unused.join(', ')}.`);
    return parts.join(' ');
  });

  readonly availableProfiles = computed(() => this.store.profiles().filter(p => !this.form().profiles.includes(p.id)));
  readonly grantedProfiles = computed(() =>
    this.form().profiles.map(id => ({ id, name: this.store.profiles().find(p => p.id === id)?.name ?? id })));

  constructor() {
    effect(() => this.store.setDirty('tool', this.dirty()));
    inject(DestroyRef).onDestroy(() => this.store.setDirty('tool', false));
    // Another tool in the editor is another test: arguments and answer of the last one go.
    effect(() => {
      this.toolId();
      untracked(() => { this.testValues.set({}); this.result.set(null); this.testError.set(null); });
    });
  }

  // ── Editing ─────────────────────────────────────────────────────────────────

  patch(change: Partial<ToolForm>): void {
    this.form.update(f => ({ ...f, ...change }));
  }

  asCount(value: unknown): number {
    const n = Math.floor(Number(value));
    return Number.isFinite(n) && n > 0 ? n : 0;
  }

  addParameter(): void {
    this.patch({
      parameters: [...this.form().parameters, { name: '', type: 'string', required: false, description: '', values: '', default: '' }],
    });
  }

  patchParameter(index: number, change: Partial<ParameterForm>): void {
    this.patch({ parameters: this.form().parameters.map((p, i) => (i === index ? { ...p, ...change } : p)) });
  }

  removeParameter(index: number): void {
    this.patch({ parameters: this.form().parameters.filter((_, i) => i !== index) });
  }

  addField(): void {
    const name = this.fieldDraft().trim();
    if (name && !this.form().fields.includes(name)) this.patch({ fields: [...this.form().fields, name] });
    this.fieldDraft.set('');
  }

  removeField(name: string): void {
    this.patch({ fields: this.form().fields.filter(f => f !== name) });
  }

  addProfile(id: string): void {
    if (id && !this.form().profiles.includes(id)) this.patch({ profiles: [...this.form().profiles, id] });
  }

  removeProfile(id: string): void {
    this.patch({ profiles: this.form().profiles.filter(p => p !== id) });
  }

  discard(): void {
    this.form.set(JSON.parse(this.baseline()) as ToolForm);
    this.error.set(null);
    this.errorField.set(null);
  }

  back(): void {
    if (this.dirty() && !confirm('This tool has unsaved changes. Discard them?')) return;
    this.store.setDirty('tool', false);
    this.closed.emit();
  }

  /** The form as the API takes it; null — with the reason shown — when it cannot be sent at all. */
  private toInput(published: boolean): McpToolInput | null {
    const f = this.form();
    const window = this.windowMinutes();
    if (window === null) {
      this.fail('Write the default window with a unit: 15m, 1h, 1d.', 'defaultWindowMinutes');
      return null;
    }
    if (!f.maxEvents) {
      this.fail('Events per call is a whole number above zero.', 'maxEvents');
      return null;
    }
    const parameters: McpToolParameter[] = f.parameters.map(p => ({
      name: p.name.trim(),
      type: p.type,
      required: p.required,
      description: p.description.trim(),
      values: p.type === 'enum' ? p.values.split(',').map(v => v.trim()).filter(Boolean) : [],
      default: p.default.trim() || null,
    }));
    return {
      name: f.name.trim(), description: f.description.trim(), parameters, filter: f.filter.trim(), fields: f.fields,
      defaultWindowMinutes: window, maxEvents: f.maxEvents, order: f.order, published, profiles: f.profiles,
    };
  }

  async save(published: boolean): Promise<void> {
    this.error.set(null);
    this.errorField.set(null);
    const input = this.toInput(published);
    if (!input) return;
    this.saving.set(true);
    try {
      const saved = await this.store.saveTool(this.isNew() ? null : this.toolId(), input);
      // What the server kept is what is now unsaved-free: trimmed names, clamped numbers.
      const form = toForm(saved);
      this.form.set(form);
      this.baseline.set(JSON.stringify(form));
      this.store.setDirty('tool', false);
      this.saved.emit(saved.id);
    } catch (e) {
      const problem = mcpError(e, 'The tool could not be saved.');
      this.fail(problem.error, problem.field ?? null);
    } finally {
      this.saving.set(false);
    }
  }

  private fail(message: string, field: string | null): void {
    this.error.set(message);
    this.errorField.set(field);
  }

  // ── The test run ────────────────────────────────────────────────────────────

  private readonly pickedProfile = signal<string | null>(null);
  /** The profile the test runs as: the one picked, else one the tool is granted to, else the first there is. */
  readonly testProfile = computed(() => {
    const profiles = this.store.profiles();
    return profiles.find(p => p.id === this.pickedProfile())
        ?? profiles.find(p => this.form().profiles.includes(p.id))
        ?? profiles[0] ?? null;
  });

  /** The test's arguments as typed, by parameter name; `from` is the window every tool takes. */
  readonly testValues = signal<Record<string, string>>({});
  readonly testing = signal(false);
  readonly testError = signal<string | null>(null);
  readonly result = signal<McpToolTestResult | null>(null);

  readonly testParameters = computed(() => this.form().parameters.filter(p => p.name.trim()).map(p => ({
    name: p.name.trim(),
    type: p.type,
    options: p.type === 'enum' ? p.values.split(',').map(v => v.trim()).filter(Boolean) : p.type === 'boolean' ? ['true', 'false'] : [],
    placeholder: p.default.trim() || (p.required ? 'required' : 'optional'),
  })));

  readonly fromPlaceholder = computed(() => {
    const minutes = this.windowMinutes();
    return minutes === null ? '-1h' : '-' + fmtWindow(minutes).replace(/\s+/g, '').replace('min', 'm');
  });

  readonly resultLine = computed(() => {
    const r = this.result();
    if (!r) return 'Not run yet.';
    if (r.isError) return `${r.status} · ${fmtCount(r.elapsedMs)} ms`;
    return `${fmtCount(r.events)} ${r.events === 1 ? 'event' : 'events'} · ${fmtCount(r.masked)} ${r.masked === 1 ? 'value' : 'values'} masked`
         + ` · about ${fmtCount(r.tokens)} tokens · ${fmtCount(r.elapsedMs)} ms`;
  });

  readonly answerSegments = computed(() => tokenSegments(this.result()?.text));
  readonly boundSegments = computed(() => tokenSegments(this.result()?.filter));

  /** The tool as an assistant's tool list carries it: of the last run, else of the saved tool. */
  readonly definition = computed(() => {
    const fromRun = this.result()?.definition;
    const saved = this.tool();
    const shown = fromRun ?? (saved ? { name: saved.name, description: saved.description, inputSchema: saved.inputSchema } : null);
    return shown ? JSON.stringify(shown, null, 2) : '';
  });

  pickProfile(id: string): void {
    this.pickedProfile.set(id);
  }

  /** What was typed for an argument of the test; empty until something is. */
  testValue(name: string): string {
    return (this.testValues() as Record<string, string | undefined>)[name] ?? '';
  }

  setTestValue(name: string, value: string): void {
    this.testValues.update(values => ({ ...values, [name]: value }));
  }

  async run(): Promise<void> {
    const profile = this.testProfile();
    if (!profile) { this.testError.set('Create a profile first: a test runs as one.'); return; }
    this.error.set(null);
    this.errorField.set(null);
    const tool = this.toInput(false);
    if (!tool) return;

    const values = this.testValues();
    const args: Record<string, unknown> = {};
    for (const p of this.testParameters()) {
      const text = (values[p.name] ?? '').trim();
      if (!text) continue;
      args[p.name] = p.type === 'boolean' ? text === 'true'
                   : p.type === 'number' && Number.isFinite(Number(text)) ? Number(text)
                   : text;
    }
    const from = (values['from'] ?? '').trim();
    if (from) args['from'] = from;

    this.testing.set(true);
    this.testError.set(null);
    try {
      this.result.set(await new Promise<McpToolTestResult>((resolve, reject) =>
        this.api.testTool({ tool, id: this.isNew() ? undefined : this.toolId(), profileId: profile.id, arguments: args })
          .subscribe({ next: resolve, error: reject })));
    } catch (e) {
      // A definition that cannot be saved cannot be run either: the same sentence, the same field.
      const problem = mcpError(e, 'The test could not be run.');
      this.testError.set(problem.error);
      this.errorField.set(problem.field ?? null);
    } finally {
      this.testing.set(false);
    }
  }
}
