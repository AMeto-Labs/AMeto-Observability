import { Pipe, PipeTransform } from '@angular/core';
import { format } from 'date-fns';

@Pipe({ name: 'eventTime' })
export class EventTimePipe implements PipeTransform {
  transform(isoString: string | null | undefined): string {
    if (!isoString) return '';
    try {
      return format(new Date(isoString), 'HH:mm:ss.SSS');
    } catch {
      return isoString ?? '';
    }
  }
}
