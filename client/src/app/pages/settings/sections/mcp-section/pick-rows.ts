/**
 * The keyboard of a list whose rows are picked (the profiles, the record of calls). The list is
 * one stop for the Tab key — the picked row has `tabindex="0"`, the others `-1` — and inside it
 * the arrows walk: fifty rows are not fifty presses of Tab on the way to what follows them.
 */

/**
 * The row a key pressed on row `index` asks for: the neighbour for Arrow Up and Down (the same
 * row at either end of the list), the row itself for Enter and Space. Null for any other key.
 */
export function rowKeyTarget(event: KeyboardEvent, index: number, count: number): number | null {
  switch (event.key) {
    case 'ArrowDown': return Math.min(index + 1, count - 1);
    case 'ArrowUp':   return Math.max(index - 1, 0);
    case 'Enter':
    case ' ':         return index;
    default:          return null;
  }
}

/**
 * Moves the focus `step` rows from the row the key was pressed on, so the next arrow starts
 * from the row just picked. Rows are siblings; a header before them is not one of them.
 */
export function focusRow(event: KeyboardEvent, step: number): void {
  let row = event.currentTarget as Element | null;
  for (let left = Math.abs(step); row && left > 0; left--) {
    row = step > 0 ? row.nextElementSibling : row.previousElementSibling;
  }
  if (step !== 0 && row instanceof HTMLElement && row.classList.contains('pick')) row.focus();
}
