import { Directive, ElementRef, inject, input, output } from '@angular/core';

/** Formats a text input as  yyyy-MM-dd ,  HH:mm , or  yyyy-MM-dd HH:mm  while typing.
 *  dateMaskType="date"     → yyyy-MM-dd
 *  dateMaskType="time"     → HH:mm
 *  dateMaskType="datetime" → yyyy-MM-dd HH:mm  (emits suggestion when date part complete)
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

  dateMaskType = input<'date' | 'time' | 'datetime'>('date');
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
    } else if (type === 'time') {
      const digits = oldVal.replace(/\D/g, '').slice(0, 4);
      for (let i = 0; i < digits.length; i++) {
        if (i === 2) formatted += ':';
        formatted += digits[i];
      }
    } else {
      // datetime: yyyy-MM-dd HH:mm (12 significant digits)
      const digits = oldVal.replace(/\D/g, '').slice(0, 12);
      for (let i = 0; i < digits.length; i++) {
        if (i === 4 || i === 6) formatted += '-';
        else if (i === 8) formatted += ' ';
        else if (i === 10) formatted += ':';
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

    // Progressive suggestions for datetime input
    if (type === 'datetime') {
      if (formatted.length === 7) {
        // yyyy-MM complete → suggest today's day
        const today = new Date();
        const day = String(today.getDate()).padStart(2, '0');
        this.activeSuggestion = `-${day}`;
        this.suggestionChange.emit(`-${day}`);
      } else if (formatted.length === 10) {
        // yyyy-MM-dd complete → suggest default time
        const defaultTime = this.dateMaskMode() === 'end' ? '23:59' : '00:00';
        this.activeSuggestion = ` ${defaultTime}`;
        this.suggestionChange.emit(` ${defaultTime}`);
      } else {
        this.activeSuggestion = '';
        this.suggestionChange.emit('');
      }
    } else {
      this.activeSuggestion = '';
      this.suggestionChange.emit('');
    }
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
