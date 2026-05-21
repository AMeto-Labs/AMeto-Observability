import { Directive, ElementRef, inject, input, output } from '@angular/core';

/** Formats a text input as  yyyy-MM-dd  or  HH:mm  while typing.
 *  dateMaskType="date"  → digits auto-formatted as yyyy-MM-dd
 *  dateMaskType="time"  → digits auto-formatted as HH:mm
 */
@Directive({
  selector: '[dateMask]',
  host: {
    '(input)':   'handle($event)',
    '(keydown)': 'onKeydown($event)',
  },
})
export class DateMaskDirective {
  private el = inject(ElementRef<HTMLInputElement>);

  dateMaskType = input<'date' | 'time'>('date');
  /** 'start' auto-completes time as 00:00, 'end' as 23:59 */
  dateMaskMode = input<'start' | 'end'>('start');

  readonly valueChange      = output<string>();
  readonly suggestionChange = output<string>();

  private activeSuggestion = '';

  handle(_e: Event): void {
    const inp    = this.el.nativeElement;
    const pos    = inp.selectionStart ?? inp.value.length;
    const oldVal = inp.value;
    const type   = this.dateMaskType();

    let formatted = '';
    if (type === 'date') {
      const digits = oldVal.replace(/\D/g, '').slice(0, 8);
      for (let i = 0; i < digits.length; i++) {
        if (i === 4 || i === 6) formatted += '-';
        formatted += digits[i];
      }
    } else {
      const digits = oldVal.replace(/\D/g, '').slice(0, 4);
      for (let i = 0; i < digits.length; i++) {
        if (i === 2) formatted += ':';
        formatted += digits[i];
      }
    }

    inp.value = formatted;

    // Reposition cursor after non-digit separators
    const digitsBeforePos = oldVal.slice(0, pos).replace(/\D/g, '').length;
    let dc = 0, newPos = formatted.length;
    for (let i = 0; i < formatted.length; i++) {
      if (/\d/.test(formatted[i]) && ++dc === digitsBeforePos) { newPos = i + 1; break; }
    }
    inp.setSelectionRange(newPos, newPos);

    this.activeSuggestion = '';
    this.suggestionChange.emit('');
    this.valueChange.emit(formatted);
  }

  onKeydown(e: KeyboardEvent): void {
    if (e.key === 'Tab' && this.activeSuggestion) {
      e.preventDefault();
      const inp      = this.el.nativeElement;
      const newValue = inp.value + this.activeSuggestion;
      inp.value      = newValue;
      this.activeSuggestion = '';
      this.suggestionChange.emit('');
      this.valueChange.emit(newValue);
      inp.setSelectionRange(newValue.length, newValue.length);
    }
  }
}
