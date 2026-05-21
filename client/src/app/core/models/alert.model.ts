export interface AlertChannel { type: string; }

export interface WebhookChannel extends AlertChannel {
  type: 'webhook';
  url: string;
  headers?: Record<string, string>;
}

export interface SmtpChannel extends AlertChannel {
  type: 'smtp';
  host: string;
  port: number;
  useSsl: boolean;
  username?: string;
  password?: string;
  from: string;
  to: string;
}

export interface AlertRule {
  id: string;
  name: string;
  filter?: string;
  threshold: number;
  window: string;
  cooldown: string;
  enabled: boolean;
  channels: AlertChannel[];
}

export interface AlertRuleUpsertRequest {
  id?: string;
  name: string;
  filter?: string;
  threshold?: number;
  windowSeconds?: number;
  cooldownSeconds?: number;
  enabled?: boolean;
  channels?: AlertChannel[];
}
