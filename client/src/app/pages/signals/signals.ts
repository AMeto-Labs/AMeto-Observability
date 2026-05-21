import {
  Component, signal, inject, OnInit,
  ChangeDetectionStrategy, ChangeDetectorRef,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { LucideAngularModule } from 'lucide-angular';

import { ApiService } from '../../core/services/api.service';
import {
  AlertRule, AlertRuleUpsertRequest,
  WebhookChannel, SmtpChannel, AlertChannel,
} from '../../core/models/alert.model';
import { fmtDuration, tsToSec } from '../../shared/utils/format';
import { EmptyStateComponent, PageHeaderComponent, SectionComponent } from '../../shared/components/ui';

// ── Channel draft (mutable form state) ───────────────────────────────────────

interface ChannelDraft {
  type: 'webhook' | 'smtp';
  // webhook
  url: string;
  // smtp
  host: string;
  port: number;
  useSsl: boolean;
  smtpUsername: string;
  smtpPassword: string;
  from: string;
  to: string;
}

function blankDraft(): ChannelDraft {
  return { type: 'webhook', url: '', host: '', port: 587, useSsl: true,
           smtpUsername: '', smtpPassword: '', from: '', to: '' };
}

function channelToDraft(c: AlertChannel): ChannelDraft {
  const d = blankDraft();
  d.type = c.type as 'webhook' | 'smtp';
  if (c.type === 'webhook') {
    d.url = (c as WebhookChannel).url ?? '';
  } else {
    const s = c as SmtpChannel;
    d.host = s.host ?? '';  d.port = s.port ?? 587;
    d.useSsl = s.useSsl ?? true;
    d.smtpUsername = s.username ?? '';
    d.smtpPassword = s.password ?? '';
    d.from = s.from ?? '';  d.to = s.to ?? '';
  }
  return d;
}

function draftToChannel(d: ChannelDraft): WebhookChannel | SmtpChannel {
  if (d.type === 'webhook') {
    return { type: 'webhook', url: d.url };
  }
  return {
    type: 'smtp', host: d.host, port: d.port, useSsl: d.useSsl,
    username: d.smtpUsername || undefined,
    password: d.smtpPassword || undefined,
    from: d.from, to: d.to,
  };
}

// ── Component ─────────────────────────────────────────────────────────────────

@Component({
  selector: 'app-signals-page',
  imports: [FormsModule, LucideAngularModule, EmptyStateComponent, PageHeaderComponent, SectionComponent],
  templateUrl: './signals.html',
  styleUrl: './signals.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class SignalsPageComponent implements OnInit {
  private api = inject(ApiService);
  private cdr = inject(ChangeDetectorRef);

  signals  = signal<AlertRule[]>([]);
  loading  = signal(false);
  saving   = signal(false);
  showForm = signal(false);
  editId   = signal<string | null>(null);

  // ── Form state (plain props — ngModel binds directly) ─────────────────────
  formName             = '';
  formFilter           = '';
  formThreshold        = 1;
  formWindowSeconds    = 300;
  formCooldownSeconds  = 900;
  formEnabled          = true;
  formChannels: ChannelDraft[] = [];

  readonly fmtDuration = fmtDuration;

  ngOnInit(): void { this.load(); }

  load(): void {
    this.loading.set(true);
    this.api.getSignals().subscribe({
      next: list => { this.signals.set(list); this.loading.set(false); this.cdr.markForCheck(); },
      error: ()   => { this.loading.set(false); this.cdr.markForCheck(); },
    });
  }

  openCreate(): void {
    this.editId.set(null);
    this.formName = ''; this.formFilter = '';
    this.formThreshold = 1; this.formWindowSeconds = 300;
    this.formCooldownSeconds = 900; this.formEnabled = true;
    this.formChannels = [];
    this.showForm.set(true);
  }

  openEdit(s: AlertRule): void {
    this.editId.set(s.id);
    this.formName            = s.name;
    this.formFilter          = s.filter ?? '';
    this.formThreshold       = s.threshold;
    this.formWindowSeconds   = tsToSec(s.window);
    this.formCooldownSeconds = tsToSec(s.cooldown);
    this.formEnabled         = s.enabled;
    this.formChannels        = s.channels.map(channelToDraft);
    this.showForm.set(true);
  }

  closeForm(): void { this.showForm.set(false); }

  save(): void {
    if (!this.formName.trim()) return;
    const req: AlertRuleUpsertRequest = {
      name:            this.formName.trim(),
      filter:          this.formFilter || undefined,
      threshold:       this.formThreshold,
      windowSeconds:   this.formWindowSeconds,
      cooldownSeconds: this.formCooldownSeconds,
      enabled:         this.formEnabled,
      channels:        this.formChannels.map(draftToChannel),
    };
    this.saving.set(true);
    const id  = this.editId();
    const obs = id ? this.api.updateSignal(id, req) : this.api.createSignal(req);
    obs.subscribe({
      next: () => { this.saving.set(false); this.showForm.set(false); this.load(); },
      error: () => { this.saving.set(false); this.cdr.markForCheck(); },
    });
  }

  toggle(s: AlertRule): void {
    this.api.updateSignal(s.id, { name: s.name, enabled: !s.enabled })
      .subscribe(() => this.load());
  }

  delete(id: string): void {
    this.api.deleteSignal(id).subscribe(() => this.load());
  }

  addChannel(): void {
    this.formChannels = [...this.formChannels, blankDraft()];
    this.cdr.markForCheck();
  }

  removeChannel(i: number): void {
    this.formChannels = this.formChannels.filter((_, idx) => idx !== i);
    this.cdr.markForCheck();
  }

  setChannelType(i: number, type: 'webhook' | 'smtp'): void {
    this.formChannels = this.formChannels.map((ch, idx) =>
      idx === i ? { ...ch, type } : ch);
    this.cdr.markForCheck();
  }
}
