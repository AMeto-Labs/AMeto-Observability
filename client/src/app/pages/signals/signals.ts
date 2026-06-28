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
  TraceMetricKind, AlertPreviewResult,
} from '../../core/models/alert.model';

type Tab = 'rules' | 'history' | 'silences';

interface ChannelDraft {
  type: 'webhook' | 'smtp' | 'telegram';
  url: string;
  host: string; port: number; useSsl: boolean; username: string; password: string; from: string; to: string;
  botToken: string; chatId: string;
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

  editing  = signal<RuleDraft | null>(null);
  preview  = signal<AlertPreviewResult | null>(null);
  previewing = signal(false);

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
    if (t === 'silences') this.loadSilences();
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

  // ── Rule list helpers ───────────────────────────────────────────────────────
  stateOf(id: string): AlertStateSnapshot | undefined { return this.states()[id]; }
  stateLabel(id: string): string { return this.stateOf(id)?.state ?? 'Ok'; }
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
  newRule() { this.editing.set(this.blankDraft()); this.preview.set(null); }
  edit(r: AlertRule) { this.editing.set(this.fromRule(r)); this.preview.set(null); }
  cancel() { this.editing.set(null); this.preview.set(null); }

  addChannel(type: 'webhook' | 'smtp' | 'telegram') {
    const d = this.editing(); if (!d) return;
    d.channels = [...d.channels, { type, url: '', host: '', port: 587, useSsl: true, username: '', password: '', from: '', to: '', botToken: '', chatId: '' }];
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

  // ── Mapping ───────────────────────────────────────────────────────────────
  private blankDraft(): RuleDraft {
    return {
      name: '', enabled: true, severity: 'Warning', source: 'Metric',
      comparator: 'GreaterOrEqual', threshold: 1, windowSeconds: 300, forSeconds: 0, cooldownSeconds: 900,
      filter: '', noData: false, metric: '', aggregation: 'quantile', quantile: 0.95,
      service: '', traceMetric: 'ErrorRatePct', template: '', channels: [],
    };
  }
  private fromRule(r: AlertRule): RuleDraft {
    return {
      id: r.id, name: r.name, enabled: r.enabled, severity: r.severity, source: r.source,
      comparator: r.comparator, threshold: r.threshold,
      windowSeconds: this.secs(r.window, 300), forSeconds: this.secs(r.for, 0), cooldownSeconds: this.secs(r.cooldown, 900),
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
    };
  }
  private draftToChannel(c: ChannelDraft): AlertChannel {
    if (c.type === 'webhook')  return { type: 'webhook', url: c.url } as any;
    if (c.type === 'telegram') return { type: 'telegram', botToken: c.botToken, chatId: c.chatId } as any;
    return { type: 'smtp', host: c.host, port: +c.port, useSsl: c.useSsl, username: c.username, password: c.password, from: c.from, to: c.to } as any;
  }
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
