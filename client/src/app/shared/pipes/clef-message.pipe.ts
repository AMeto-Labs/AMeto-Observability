import { Pipe, PipeTransform, inject } from '@angular/core';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { renderMessageHtml } from '../utils/clef-renderer';
import { EventDto } from '../../core/models/event.model';

@Pipe({ name: 'clefMessage' })
export class ClefMessagePipe implements PipeTransform {
  private sanitizer = inject(DomSanitizer);

  transform(event: EventDto): SafeHtml {
    const html = renderMessageHtml(event['@mt'], event.props);
    return this.sanitizer.bypassSecurityTrustHtml(html);
  }
}
