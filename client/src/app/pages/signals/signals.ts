import {
  Component, signal, computed, inject, OnInit, OnDestroy,
  ChangeDetectionStrategy, ChangeDetectorRef,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';
import { format } from 'date-fns';
import { ApiService } from '../../core/services/api.service';
import {
  AlertRule, AlertRuleUpsertRequest, AlertStateSnapshot, AlertHistoryEntry,
  AlertSilence, AlertChannel, AlertSource, AlertSeverity, AlertComparator,
  TraceMetricKind, AlertPreviewResult, MaintenanceWindow,
} from '../../core/models/alert.model';

interface MaintDraft { name: string; days: boolean[]; startTime: string; durationMinutes: number; maxSeverity: string; }

type Tab = 'rules' | 'history' | 'silences';

type ChannelType = 'webhook' | 'smtp' | 'telegram' | 'slack' | 'discord' | 'teams' | 'pagerduty' | 'httpflow';

interface HeaderKV { key: string; value: string; }
interface ExtractDraft { var: string; source: string; expr: string; }
interface StepDraft { name: string; method: string; url: string; headers: HeaderKV[]; bodyType: string; body: string; extracts: ExtractDraft[]; }
interface SecretKV { name: string; value: string; }

interface ChannelDraft {
  type: ChannelType;
  url: string;
  host: string; port: number; useSsl: boolean; username: string; password: string; from: string; to: string;
  botToken: string; chatId: string;
  webhookUrl: string; routingKey: string;
  escalationOnly: boolean; minSeverity: string;
  steps: StepDraft[]; secrets: SecretKV[];
}

interface RuleDraft {
  id?: string;
  name: string;
  enabled: boolean;
  severity: AlertSeverity;
  source: AlertSource;
  comparator: AlertComparator;
  threshold: number;
  windowSeconds: number;
  forSeconds: number;
  cooldownSeconds: number;
  repeatSeconds: number;
  escalateSeconds: number;
  filter: string;
  noData: boolean;
  metric: string;
  aggregation: string;
  quantile: number;
  service: string;
  traceMetric: TraceMetricKind;
  template: string;
  channels: ChannelDraft[];
}

@Component({
  selector: 'app-signals-page',
  imports: [FormsModule, LucideAngularModule],
  templateUrl: './signals.html',
  styleUrl: './signals.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class SignalsPageComponent implements OnInit, OnDestroy {
  private api = inject(ApiService);
  private cdr = inject(ChangeDetectorRef);

  tab      = signal<Tab>('rules');
  loading  = signal(false);
  rules    = signal<AlertRule[]>([]);
  states   = signal<Record<string, AlertStateSnapshot>>({});
  history  = signal<AlertHistoryEntry[]>([]);
  silences = signal<AlertSilence[]>([]);
  maintenance = signal<MaintenanceWindow[]>([]);
  maintDraft: MaintDraft = this.blankMaint();
  readonly dayLabels = ['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'];
  // Documented literally (interpolation emits the {{…}} as text) — not evaluated as template expressions.
  readonly hfVarsHint = 'Substitute anywhere with {{var}}. Built-in: {{alert.name}} {{alert.value}} {{alert.severity}} {{alert.state}} {{alert.threshold}} {{alert.message}} {{alert.at}}. Secret vars: {{secret.NAME}}.';

  editing  = signal<RuleDraft | null>(null);
  preview  = signal<AlertPreviewResult | null>(null);
  previewing = signal(false);
  testStatus = signal<string>('');

  readonly sources: AlertSource[] = ['Log', 'Metric', 'Trace'];
  readonly severities: AlertSeverity[] = ['Info', 'Warning', 'Critical'];
  readonly comparators: { v: AlertComparator; label: string }[] = [
    { v: 'GreaterThan', label: '>' }, { v: 'GreaterOrEqual', label: '≥' },
    { v: 'LessThan', label: '<' }, { v: 'LessOrEqual', label: '≤' },
  ];
  readonly aggregations = ['last', 'avg', 'min', 'max', 'sum', 'rate', 'increase', 'quantile'];
  readonly traceMetrics: TraceMetricKind[] = ['ErrorRatePct', 'P50Ms', 'P95Ms', 'P99Ms', 'SpanCount'];

  firingCount = computed(() =>
    Object.values(this.states()).filter(s => s.state === 'Firing').length);

  private _poll: ReturnType<typeof setInterval> | null = null;

  ngOnInit() {
    this.reload();
    this._poll = setInterval(() => this.refreshLive(), 15_000);
  }
  ngOnDestroy() { if (this._poll) clearInterval(this._poll); }

  setTab(t: Tab) {
    this.tab.set(t);
    if (t === 'history') this.loadHistory();
    if (t === 'silences') { this.loadSilences(); this.loadMaintenance(); }
  }

  reload() {
    this.loading.set(true);
    this.api.getAlerts().subscribe({
      next: r => { this.rules.set(r); this.loading.set(false); this.refreshLive(); this.cdr.markForCheck(); },
      error: () => { this.loading.set(false); this.cdr.markForCheck(); },
    });
  }
  private refreshLive() {
    this.api.getAlertState().subscribe(s => {
      const map: Record<string, AlertStateSnapshot> = {};
      for (const x of s) map[x.ruleId] = x;
      this.states.set(map); this.cdr.markForCheck();
    });
  }
  loadHistory() { this.api.getAlertHistory(200).subscribe(h => { this.history.set(h); this.cdr.markForCheck(); }); }
  loadSilences() { this.api.getAlertSilences().subscribe(s => { this.silences.set(s); this.cdr.markForCheck(); }); }
  loadMaintenance() { this.api.getMaintenance().subscribe(m => { this.maintenance.set(m); this.cdr.markForCheck(); }); }

  // ── Maintenance windows ─────────────────────────────────────────────────────
  private blankMaint(): MaintDraft {
    return { name: '', days: [true, true, true, true, true, true, true], startTime: '02:00', durationMinutes: 60, maxSeverity: '' };
  }
  toggleMaintDay(i: number) { this.maintDraft.days[i] = !this.maintDraft.days[i]; }
  addMaintenance() {
    const d = this.maintDraft;
    if (!d.name.trim()) return;
    const [h, m] = d.startTime.split(':').map(Number);
    const w: MaintenanceWindow = {
      id: '', name: d.name.trim(), enabled: true,
      daysOfWeek: d.days.reduce((mask, on, i) => on ? mask | (1 << i) : mask, 0) || 127,
      startMinuteUtc: (h || 0) * 60 + (m || 0),
      durationMinutes: +d.durationMinutes || 60,
      maxSeverity: d.maxSeverity ? (d.maxSeverity as AlertSeverity) : null,
    };
    this.api.createMaintenance(w).subscribe(() => { this.maintDraft = this.blankMaint(); this.loadMaintenance(); });
  }
  removeMaintenance(id: string) { this.api.deleteMaintenance(id).subscribe(() => this.loadMaintenance()); }

  maintSchedule(w: MaintenanceWindow): string {
    const days = this.maintDaysLabel(w.daysOfWeek);
    const end = (w.startMinuteUtc + w.durationMinutes) % 1440;
    return `${days} · ${this.fmtMin(w.startMinuteUtc)}–${this.fmtMin(end)} UTC`;
  }
  private maintDaysLabel(mask: number): string {
    if (mask === 127) return 'Every day';
    if (mask === 0b0111110) return 'Weekdays';
    if (mask === 0b1000001) return 'Weekends';
    return this.dayLabels.filter((_, i) => mask & (1 << i)).join(' ');
  }
  private fmtMin(m: number): string {
    return `${String(Math.floor(m / 60)).padStart(2, '0')}:${String(m % 60).padStart(2, '0')}`;
  }
  maintActive(w: MaintenanceWindow): boolean {
    if (!w.enabled) return false;
    const now = new Date();
    const nowMin = now.getUTCHours() * 60 + now.getUTCMinutes();
    const nowDow = now.getUTCDay();
    for (let off = 0; off <= 1; off++) {
      const startDow = (nowDow - off + 7) % 7;
      if (!(w.daysOfWeek & (1 << startDow))) continue;
      const elapsed = nowMin + off * 1440 - w.startMinuteUtc;
      if (elapsed >= 0 && elapsed < w.durationMinutes) return true;
    }
    return false;
  }
  maintSevLabel(w: MaintenanceWindow): string { return w.maxSeverity ? `≤ ${w.maxSeverity}` : 'all'; }

  // ── Rule list helpers ───────────────────────────────────────────────────────
  stateOf(id: string): AlertStateSnapshot | undefined { return this.states()[id]; }
  stateLabel(id: string): string { return this.stateOf(id)?.state ?? 'Ok'; }
  isFiring(id: string): boolean { return this.stateOf(id)?.state === 'Firing'; }
  isAcked(id: string): boolean { return !!this.stateOf(id)?.ackedAt; }
  ackedBy(id: string): string { return this.stateOf(id)?.ackedBy ?? ''; }

  ack(id: string, e: Event) {
    e.stopPropagation();
    this.api.ackAlert(id).subscribe(() => this.refreshState());
  }
  unack(id: string, e: Event) {
    e.stopPropagation();
    this.api.unackAlert(id).subscribe(() => this.refreshState());
  }
  private refreshState() {
    this.api.getAlertState().subscribe(s => {
      const map: Record<string, AlertStateSnapshot> = {};
      for (const x of s) map[x.ruleId] = x;
      this.states.set(map); this.cdr.markForCheck();
    });
  }
  stateCls(id: string): string {
    const s = this.stateOf(id)?.state ?? 'Ok';
    return s === 'Firing' ? 'st-firing' : s === 'Pending' ? 'st-pending' : s === 'NoData' ? 'st-nodata' : 'st-ok';
  }
  lastValue(id: string): string {
    const v = this.stateOf(id)?.lastValue;
    return v == null ? '—' : (v === Math.floor(v) ? String(v) : v.toFixed(2));
  }
  sevCls(s: AlertSeverity): string { return 'sev-' + s.toLowerCase(); }
  cmpLabel(c: AlertComparator): string { return this.comparators.find(x => x.v === c)?.label ?? c; }

  toggleEnabled(r: AlertRule) {
    const req = this.toRequest(this.fromRule(r));
    req.enabled = !r.enabled;
    this.api.updateAlert(r.id, req).subscribe(() => this.reload());
  }
  remove(r: AlertRule) {
    if (!confirm(`Delete rule "${r.name}"?`)) return;
    this.api.deleteAlert(r.id).subscribe(() => this.reload());
  }
  silence(r: AlertRule) {
    const mins = Number(prompt(`Silence "${r.name}" for how many minutes?`, '60'));
    if (!mins || mins <= 0) return;
    this.api.createAlertSilence(r.id, mins).subscribe(() => { if (this.tab() === 'silences') this.loadSilences(); });
  }
  removeSilence(s: AlertSilence) {
    this.api.deleteAlertSilence(s.id).subscribe(() => this.loadSilences());
  }
  ruleName(id: string): string { return this.rules().find(r => r.id === id)?.name ?? id; }

  // ── Editor ──────────────────────────────────────────────────────────────────
  newRule() { this.editing.set(this.blankDraft()); this.preview.set(null); this.testStatus.set(''); }
  edit(r: AlertRule) { this.editing.set(this.fromRule(r)); this.preview.set(null); this.testStatus.set(''); }
  cancel() { this.editing.set(null); this.preview.set(null); this.testStatus.set(''); }

  addChannel(type: ChannelType) {
    const d = this.editing(); if (!d) return;
    d.channels = [...d.channels, { type, url: '', host: '', port: 587, useSsl: true, username: '', password: '', from: '', to: '', botToken: '', chatId: '', webhookUrl: '', routingKey: '', escalationOnly: false, minSeverity: '', steps: [], secrets: [] }];
    this.editing.set({ ...d });
  }
  removeChannel(i: number) {
    const d = this.editing(); if (!d) return;
    d.channels = d.channels.filter((_, idx) => idx !== i);
    this.editing.set({ ...d });
  }

  runPreview() {
    const d = this.editing(); if (!d) return;
    this.previewing.set(true);
    this.api.previewAlert(this.toRequest(d)).subscribe({
      next: p => { this.preview.set(p); this.previewing.set(false); this.cdr.markForCheck(); },
      error: () => { this.previewing.set(false); this.cdr.markForCheck(); },
    });
  }

  save() {
    const d = this.editing(); if (!d || !d.name.trim()) return;
    const req = this.toRequest(d);
    const op = d.id ? this.api.updateAlert(d.id, req) : this.api.createAlert(req);
    op.subscribe(() => { this.editing.set(null); this.preview.set(null); this.reload(); });
  }

  sendTest() {
    const d = this.editing(); if (!d) return;
    if (!d.channels.length) { this.testStatus.set('Add a channel first'); return; }
    this.testStatus.set('Sending…');
    this.api.testAlert(this.toRequest(d)).subscribe({
      next: r => { this.testStatus.set(`Test sent to ${r.sent} channel(s) — check they arrived`); this.cdr.markForCheck(); },
      error: e => { this.testStatus.set(e?.error ?? 'Test failed'); this.cdr.markForCheck(); },
    });
  }

  // ── Mapping ───────────────────────────────────────────────────────────────
  private blankDraft(): RuleDraft {
    return {
      name: '', enabled: true, severity: 'Warning', source: 'Metric',
      comparator: 'GreaterOrEqual', threshold: 1, windowSeconds: 300, forSeconds: 0, cooldownSeconds: 900, repeatSeconds: 0, escalateSeconds: 0,
      filter: '', noData: false, metric: '', aggregation: 'quantile', quantile: 0.95,
      service: '', traceMetric: 'ErrorRatePct', template: '', channels: [],
    };
  }
  private fromRule(r: AlertRule): RuleDraft {
    return {
      id: r.id, name: r.name, enabled: r.enabled, severity: r.severity, source: r.source,
      comparator: r.comparator, threshold: r.threshold,
      windowSeconds: this.secs(r.window, 300), forSeconds: this.secs(r.for, 0), cooldownSeconds: this.secs(r.cooldown, 900),
      repeatSeconds: this.secs(r.repeatInterval, 0), escalateSeconds: this.secs(r.escalateAfter, 0),
      filter: r.filter ?? '', noData: r.noData, metric: r.metric ?? '', aggregation: r.aggregation ?? 'last',
      quantile: r.quantile ?? 0.95, service: r.service ?? '', traceMetric: r.traceMetric, template: r.template ?? '',
      channels: r.channels.map(c => this.channelToDraft(c)),
    };
  }
  private toRequest(d: RuleDraft): AlertRuleUpsertRequest {
    return {
      id: d.id, name: d.name, enabled: d.enabled, severity: d.severity, source: d.source,
      comparator: d.comparator, threshold: +d.threshold,
      windowSeconds: +d.windowSeconds, forSeconds: +d.forSeconds, cooldownSeconds: +d.cooldownSeconds,
      repeatSeconds: +d.repeatSeconds, escalateSeconds: +d.escalateSeconds,
      filter: d.source === 'Log' ? d.filter : undefined,
      noData: d.source === 'Log' ? d.noData : undefined,
      metric: d.source === 'Metric' ? d.metric : undefined,
      aggregation: d.source === 'Metric' ? d.aggregation : undefined,
      quantile: d.source === 'Metric' && d.aggregation === 'quantile' ? +d.quantile : undefined,
      service: d.source === 'Trace' ? d.service : undefined,
      traceMetric: d.source === 'Trace' ? d.traceMetric : undefined,
      template: d.template || undefined,
      channels: d.channels.map(c => this.draftToChannel(c)),
    };
  }
  private channelToDraft(c: any): ChannelDraft {
    return {
      type: c.type, url: c.url ?? '', host: c.host ?? '', port: c.port ?? 587, useSsl: c.useSsl ?? true,
      username: c.username ?? '', password: c.password ?? '', from: c.from ?? '', to: c.to ?? '',
      botToken: c.botToken ?? '', chatId: c.chatId ?? '',
      webhookUrl: c.webhookUrl ?? '', routingKey: c.routingKey ?? '',
      escalationOnly: c.escalationOnly ?? false, minSeverity: c.minSeverity ?? '',
      steps: (c.steps ?? []).map((s: any) => ({
        name: s.name ?? '', method: s.method ?? 'POST', url: s.url ?? '',
        headers: (s.headers ?? []).map((h: any) => ({ key: h.key ?? '', value: h.value ?? '' })),
        bodyType: s.bodyType ?? 'none', body: s.body ?? '',
        extracts: (s.extracts ?? []).map((e: any) => ({ var: e.var ?? '', source: e.source ?? 'json', expr: e.expr ?? '' })),
      })),
      secrets: Object.entries(c.secrets ?? {}).map(([name, value]) => ({ name, value: value as string })),
    };
  }
  private draftToChannel(c: ChannelDraft): AlertChannel {
    const esc = { escalationOnly: c.escalationOnly, minSeverity: c.minSeverity || undefined };
    switch (c.type) {
      case 'webhook':   return { type: 'webhook', url: c.url, ...esc } as any;
      case 'telegram':  return { type: 'telegram', botToken: c.botToken, chatId: c.chatId, ...esc } as any;
      case 'slack':     return { type: 'slack', webhookUrl: c.webhookUrl, ...esc } as any;
      case 'discord':   return { type: 'discord', webhookUrl: c.webhookUrl, ...esc } as any;
      case 'teams':     return { type: 'teams', webhookUrl: c.webhookUrl, ...esc } as any;
      case 'pagerduty': return { type: 'pagerduty', routingKey: c.routingKey, ...esc } as any;
      case 'httpflow':  return { type: 'httpflow', ...esc,
        steps: c.steps.map(s => ({
          name: s.name, method: s.method, url: s.url,
          headers: s.headers.filter(h => h.key.trim()),
          bodyType: s.bodyType, body: s.body || undefined,
          extracts: s.extracts.filter(e => e.var.trim()),
        })),
        secrets: Object.fromEntries(c.secrets.filter(x => x.name.trim()).map(x => [x.name.trim(), x.value])),
      } as any;
      default:          return { type: 'smtp', host: c.host, port: +c.port, useSsl: c.useSsl, username: c.username, password: c.password, from: c.from, to: c.to, ...esc } as any;
    }
  }

  // ── HTTP-flow builder helpers ────────────────────────────────────────────────
  private bumpEditing() { const d = this.editing(); if (d) this.editing.set({ ...d }); }
  addStep(c: ChannelDraft)          { c.steps = [...c.steps, { name: '', method: 'POST', url: '', headers: [], bodyType: 'none', body: '', extracts: [] }]; this.bumpEditing(); }
  removeStep(c: ChannelDraft, i: number) { c.steps = c.steps.filter((_, idx) => idx !== i); this.bumpEditing(); }
  addHeader(s: StepDraft)           { s.headers = [...s.headers, { key: '', value: '' }]; this.bumpEditing(); }
  removeHeader(s: StepDraft, i: number)  { s.headers = s.headers.filter((_, idx) => idx !== i); this.bumpEditing(); }
  addExtract(s: StepDraft)          { s.extracts = [...s.extracts, { var: '', source: 'json', expr: '' }]; this.bumpEditing(); }
  removeExtract(s: StepDraft, i: number) { s.extracts = s.extracts.filter((_, idx) => idx !== i); this.bumpEditing(); }
  addSecret(c: ChannelDraft)        { c.secrets = [...c.secrets, { name: '', value: '' }]; this.bumpEditing(); }
  removeSecret(c: ChannelDraft, i: number) { c.secrets = c.secrets.filter((_, idx) => idx !== i); this.bumpEditing(); }
  secsOf(ts: string | undefined): number { return this.secs(ts, 0); }
  private secs(ts: string | undefined, def: number): number {
    if (!ts) return def;
    // "hh:mm:ss" or "d.hh:mm:ss"
    const m = ts.match(/(?:(\d+)\.)?(\d{2}):(\d{2}):(\d{2})/);
    if (!m) return def;
    const d = +(m[1] ?? 0), h = +m[2], mi = +m[3], s = +m[4];
    return d * 86400 + h * 3600 + mi * 60 + s;
  }

  // ── Formatting ──────────────────────────────────────────────────────────────
  fmtSecs(s: number): string {
    if (s % 3600 === 0 && s >= 3600) return s / 3600 + 'h';
    if (s % 60 === 0 && s >= 60) return s / 60 + 'm';
    return s + 's';
  }
  fmtNum(v: number): string { return v == null ? '—' : (v === Math.floor(v) ? String(v) : v.toFixed(2)); }
  fmtTime(iso: string): string { return iso ? format(new Date(iso), 'dd/MM HH:mm:ss') : '—'; }
  fmtAgo(iso?: string): string {
    if (!iso) return '—';
    const d = Date.now() - new Date(iso).getTime();
    if (d < 60_000) return Math.round(d / 1000) + 's ago';
    if (d < 3_600_000) return Math.round(d / 60_000) + 'm ago';
    if (d < 86_400_000) return Math.round(d / 3_600_000) + 'h ago';
    return Math.round(d / 86_400_000) + 'd ago';
  }
  sourceIcon(s: AlertSource): string { return s === 'Metric' ? 'chart-line' : s === 'Trace' ? 'git-branch' : 'scroll-text'; }
}
