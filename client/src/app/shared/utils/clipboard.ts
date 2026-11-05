/**
 * Writes `text` to the clipboard. `navigator.clipboard` needs a secure context (https or
 * localhost) and focus; a plain-http host (a stand at http://sandbox:8555) gets the legacy
 * fallback through a hidden textarea and execCommand. The one copy of this for the pages that
 * copy — the event detail, the Traces page, the message action sheet.
 */
export async function copyToClipboard(text: string): Promise<void> {
  try {
    await navigator.clipboard.writeText(text);
  } catch {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    ta.remove();
  }
}
