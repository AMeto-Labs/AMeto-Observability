/**
 * Settings → AI / MCP: the shapes of the settings API under `/api/mcp` (docs/MCP.md).
 * Names are the server's; nothing here is derived on the client.
 */

/** The ceilings no profile can raise (Connection → Server limits). */
export interface McpLimits {
  responseBudgetTokens: number;
  maxWindowMinutes: number;
  maxEvents: number;
  callsPerMinute: number;
  maxConcurrent: number;
}

/** A value detector; `id` is also what a call's masking breakdown counts its finds under. */
export interface McpDetector {
  id: string;
  name: string;
  on: boolean;
}

export interface McpBuiltInTool {
  name: string;
  title: string;
  description: string;
  /** True for the two tools that take a filter the assistant writes. */
  takesFreeFilter: boolean;
  /** False: switched off for the whole server, so offered to no profile. */
  enabled: boolean;
}

/** `GET /api/mcp`. */
export interface McpOverview {
  /** The endpoint's path below the deployment prefix: `/mcp`. */
  path: string;
  enabled: boolean;
  secretCreatedAt: string;
  limits: McpLimits;
  detectors: McpDetector[];
  tools: McpBuiltInTool[];
}

export type McpToolsMode = 'builtInAndPublished' | 'publishedOnly';
export type McpMaskingMode = 'standard' | 'strict';

export interface McpProfile {
  id: string;
  name: string;
  description: string;
  /** Eight characters of the key's hash: enough to tell two keys apart, useless as a key. */
  keyPreview: string;
  toolsMode: McpToolsMode;
  /** Built-in tools switched on for the profile, by name. */
  tools: string[];
  /** Empty: every service. */
  services: string[];
  maskingMode: McpMaskingMode;
  freeFilters: boolean;
  maxEvents: number;
  maxWindowMinutes: number;
  callsPerMinute: number;
  /** Ids of the saved tools that name this profile, drafts included. */
  publishedTools: string[];
  createdBy: string;
  createdAt: string;
  lastCallAt: string | null;
}

/** What `POST` / `PUT /api/mcp/profiles` take. */
export interface McpProfileInput {
  name: string;
  description: string;
  toolsMode: McpToolsMode;
  tools: string[];
  services: string[];
  maskingMode: McpMaskingMode;
  freeFilters: boolean;
  maxEvents: number;
  maxWindowMinutes: number;
  callsPerMinute: number;
  publishedTools: string[];
}

export type McpMaskAction = 'hash' | 'remove' | 'keepLast4' | 'keep';

export interface McpRule {
  id: string;
  field: string;
  /** Empty: every service. */
  services: string[];
  action: McpMaskAction;
}

export interface McpRuleInput {
  field: string;
  services: string[];
  action: McpMaskAction;
}

export type McpParameterType = 'string' | 'number' | 'boolean' | 'enum';

export interface McpToolParameter {
  name: string;
  type: McpParameterType;
  required: boolean;
  description: string;
  /** The allowed values of an `enum`. */
  values: string[];
  default: string | null;
}

/** A JSON Schema object as the tool list carries it: only what Settings reads. */
export interface McpInputSchema {
  properties?: Record<string, unknown>;
  required?: string[];
}

export interface McpPublishedTool {
  id: string;
  name: string;
  description: string;
  parameters: McpToolParameter[];
  filter: string;
  /** The properties an answer returns; empty: all of them. */
  fields: string[];
  defaultWindowMinutes: number;
  maxEvents: number;
  order: 'newest' | 'oldest';
  /** False: a draft, saved and offered to nobody. */
  published: boolean;
  /** Ids of the profiles that may call it. */
  profiles: string[];
  createdBy: string;
  createdAt: string;
  updatedAt: string;
  inputSchema: McpInputSchema;
}

/** What `POST` / `PUT /api/mcp/tools` take. */
export interface McpToolInput {
  name: string;
  description: string;
  parameters: McpToolParameter[];
  filter: string;
  fields: string[];
  defaultWindowMinutes: number;
  maxEvents: number;
  order: 'newest' | 'oldest';
  published: boolean;
  profiles: string[];
}

/** `GET /api/mcp/tools`. */
export interface McpToolList {
  builtIn: McpBuiltInTool[];
  published: McpPublishedTool[];
}

/** One entry of `GET /api/mcp/profiles/{id}/tools`: a tool as the profile's key is offered it. */
export interface McpOfferedTool {
  name: string;
  description: string;
  builtIn: boolean;
  inputSchema: McpInputSchema;
}

/**
 * What an answer hid, by what hid it. `rules` and `strict` count by field path, `detectors` by
 * detector id; `ruleIds` — in a call's record only — counts the rules' own hits.
 */
export interface McpMaskedBy {
  rules?: Record<string, number>;
  strict?: Record<string, number>;
  detectors?: Record<string, number>;
  ruleIds?: Record<string, number>;
}

/** `POST /api/mcp/tools/test`: the editor's test run. */
export interface McpToolTestResult {
  text: string;
  isError: boolean;
  status: string;
  /** The template with the test's arguments in place; null when they did not bind. */
  filter: string | null;
  events: number;
  masked: number;
  maskedBy: McpMaskedBy;
  tokens: number;
  elapsedMs: number;
  definition: { name: string; description: string; inputSchema: McpInputSchema };
}

/** One call from the record (`GET /api/mcp/activity`). */
export interface McpCall {
  id: string;
  at: string;
  /** The cursor to continue after this call: UtcTicks as text. */
  afterTs: string;
  tool: string | null;
  profile: string | null;
  profileId: string | null;
  status: string | null;
  events: number | null;
  masked: number | null;
  tokens: number | null;
  elapsedMs: number | null;
  /** The arguments as the assistant sent them: JSON text. */
  arguments: string | null;
  refusal: string | null;
  /** `input`, `limit`, `masking`, `scope` or `unavailable`. */
  refusalKind: string | null;
  /** The filter as written; for a published tool, its template with the arguments bound in. */
  filter: string | null;
  client: string | null;
  maskedBy: McpMaskedBy | null;
}

export interface McpActivityPage {
  calls: McpCall[];
  more: boolean;
}

export interface McpActivityQuery {
  profile?: string;
  tool?: string;
  status?: string;
  /** ISO-8601; the server's default is the last 24 hours. */
  from?: string;
  count?: number;
  afterTs?: string;
  afterId?: string;
}

/** `GET /api/mcp/summary`: the calls of a window added up. */
export interface McpSummary {
  hours: number;
  /** True when the window holds more calls than one summary reads: the figures are of the newest only. */
  partial: boolean;
  calls: number;
  profiles: number;
  events: number;
  /** Of the calls that returned any event; null when none did. */
  medianEvents: number | null;
  masked: number;
  maskedBy: { rules: number; strict: number; detectors: number };
  statuses: Record<string, number>;
  /** Refusals by kind. */
  refusals: Record<string, number>;
  /** Hidden values by the id of the rule that hid them. */
  rules: Record<string, number>;
  /** Hidden values by detector id. */
  detectors: Record<string, number>;
}

/** `GET /api/mcp/tools/usage`. */
export interface McpUsage {
  days: number;
  partial: boolean;
  calls: Record<string, number>;
}

/** One event as stored, in the Logs page's own shape. */
export interface McpStoredEvent {
  '@t': string;
  '@mt': string;
  '@l'?: string;
  '@service'?: string;
  '@x'?: { type: string; message?: string; stack?: string };
  props?: Record<string, unknown>;
}

/** `POST /api/mcp/preview`: one event as stored and as an answer would carry it. */
export interface McpPreview {
  found: boolean;
  stored?: McpStoredEvent;
  sent?: {
    template: string;
    properties: { name: string; value: string }[];
    exception?: { type: string; message?: string; stack?: string };
  };
  masked?: {
    total: number;
    rules: Record<string, number>;
    strict: Record<string, number>;
    detectors: Record<string, number>;
  };
}

/** The body of a `400` from this API: the sentence, and the field it is about. */
export interface McpFieldError {
  error: string;
  field?: string;
}
