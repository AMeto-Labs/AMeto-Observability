import { McpMaskingMode, McpProfile, McpProfilePatch, McpToolsMode } from '../../../../../core/models/mcp.model';
import { fmtWindow, sameWindow } from '../mcp-format';

/**
 * A profile as its card holds it: the window as typed, a limit as its number field holds it
 * (null while the field is empty), the grants as tool ids.
 */
export interface ProfileForm {
  name: string;
  description: string;
  toolsMode: McpToolsMode;
  builtIn: string[];
  published: string[];
  services: string[];
  maskingMode: McpMaskingMode;
  freeFilters: boolean;
  maxEvents: number | null;
  window: string;
  callsPerMinute: number | null;
}

/** A blank profile: every built-in tool on, no grants, every service, the defaults for the rest. */
export function blankForm(builtIn: string[]): ProfileForm {
  return {
    name: '', description: '', toolsMode: 'builtInAndPublished', builtIn, published: [], services: [],
    maskingMode: 'standard', freeFilters: true, maxEvents: 200, window: '24 h', callsPerMinute: 30,
  };
}

export function toForm(p: McpProfile): ProfileForm {
  return {
    name: p.name, description: p.description, toolsMode: p.toolsMode,
    builtIn: [...p.tools], published: [...p.publishedTools], services: [...p.services],
    maskingMode: p.maskingMode, freeFilters: p.freeFilters,
    maxEvents: p.maxEvents, window: fmtWindow(p.maxWindowMinutes), callsPerMinute: p.callsPerMinute,
  };
}

/** True when two lists hold the same members; the order is nobody's to save. */
export function sameMembers(a: readonly string[], b: readonly string[]): boolean {
  const members = new Set(a);
  return members.size === new Set(b).size && b.every(x => members.has(x));
}

/**
 * True when two forms describe the same profile: a switched-off-and-on-again tool changes
 * nothing, and neither does `24h` typed over `24 h`.
 */
export function sameForm(a: ProfileForm, b: ProfileForm): boolean {
  return a.name === b.name && a.description === b.description && a.toolsMode === b.toolsMode
      && a.maskingMode === b.maskingMode && a.freeFilters === b.freeFilters
      && a.maxEvents === b.maxEvents && sameWindow(a.window, b.window) && a.callsPerMinute === b.callsPerMinute
      && sameMembers(a.builtIn, b.builtIn) && sameMembers(a.published, b.published) && sameMembers(a.services, b.services);
}

/**
 * What saving the card changes: the fields where the card is not the profile it was filled
 * with, as `PUT` takes them. The server keeps every field the body leaves out, so a card
 * opened before somebody else narrowed the services or the masking cannot put the old ones
 * back by saving a new name. `maxWindowMinutes` is the card's window, read.
 */
export function changesOf(form: ProfileForm, base: ProfileForm, maxWindowMinutes: number): McpProfilePatch {
  const patch: McpProfilePatch = {};
  const name = form.name.trim();
  const description = form.description.trim();
  if (name !== base.name.trim()) patch.name = name;
  if (description !== base.description.trim()) patch.description = description;
  if (form.toolsMode !== base.toolsMode) patch.toolsMode = form.toolsMode;
  if (!sameMembers(form.builtIn, base.builtIn)) patch.tools = form.builtIn;
  if (!sameMembers(form.services, base.services)) patch.services = form.services;
  if (form.maskingMode !== base.maskingMode) patch.maskingMode = form.maskingMode;
  if (form.freeFilters !== base.freeFilters) patch.freeFilters = form.freeFilters;
  if (form.maxEvents !== null && form.maxEvents !== base.maxEvents) patch.maxEvents = form.maxEvents;
  if (!sameWindow(form.window, base.window)) patch.maxWindowMinutes = maxWindowMinutes;
  if (form.callsPerMinute !== null && form.callsPerMinute !== base.callsPerMinute) patch.callsPerMinute = form.callsPerMinute;
  // The grants as one list, and only when they were changed here: a tool's editor may have
  // changed them since the card was filled.
  if (!sameMembers(form.published, base.published)) patch.publishedTools = form.published;
  return patch;
}

/**
 * What the user did to a list — put members in, took members out — done again to the list as
 * it stands now. Somebody else's additions stay, their removals stay removed, and the user's
 * own switches are kept on top.
 */
export function rebaseList(mine: readonly string[], base: readonly string[], fresh: readonly string[]): string[] {
  const added = mine.filter(x => !base.includes(x));
  const removed = base.filter(x => !mine.includes(x));
  return [...fresh.filter(x => !removed.includes(x)), ...added.filter(x => !fresh.includes(x))];
}

/**
 * The card after its profile changed under it — a tool's editor granted or withdrew the tool,
 * a tool was deleted, the lists were read again. `base` is the profile as the card was filled
 * with it, `fresh` as it is now. What the user has not touched follows the profile; what they
 * have stays theirs, on top of it. With nothing unsaved that is simply `fresh`.
 *
 * The card must not keep the old picture instead: saving it would send that picture back, and
 * the server would re-grant what was withdrawn a moment ago.
 */
export function rebaseForm(form: ProfileForm, base: ProfileForm, fresh: ProfileForm): ProfileForm {
  const kept = <K extends keyof ProfileForm>(key: K): ProfileForm[K] => (form[key] === base[key] ? fresh[key] : form[key]);
  return {
    name: kept('name'), description: kept('description'), toolsMode: kept('toolsMode'),
    builtIn: rebaseList(form.builtIn, base.builtIn, fresh.builtIn),
    published: rebaseList(form.published, base.published, fresh.published),
    services: rebaseList(form.services, base.services, fresh.services),
    maskingMode: kept('maskingMode'), freeFilters: kept('freeFilters'),
    maxEvents: kept('maxEvents'),
    // `24h` over `24 h` is the window untouched: it follows the profile like any other field.
    window: sameWindow(form.window, base.window) ? fresh.window : form.window,
    callsPerMinute: kept('callsPerMinute'),
  };
}
