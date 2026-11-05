import { ChangeDetectionStrategy, Component, computed, inject, input, output } from '@angular/core';
import { DomSanitizer } from '@angular/platform-browser';
import { LucideAngularModule } from 'lucide-angular';

import { BottomSheetComponent } from '../../../../shared/components/bottom-sheet/bottom-sheet';
import { EventDto } from '../../../../core/models/event.model';
import { renderMessageHtml, renderTemplateHtml } from '../../../../shared/utils/clef-renderer';
import { copyToClipboard } from '../../../../shared/utils/clipboard';
import { excludeLikeFilter, findLikeFilter, quoted } from '../../store/events-filter.util';

/** The expression on one line for the row's caption, cut with an ellipsis. */
function shorten(s: string, max = 72): string {
  return s.length > max ? `${s.slice(0, max - 1)}…` : s;
}

/**
 * The message action sheet (design canvas, "Телефон — long-press по строке"): what a long press
 * on a row offers on a touch screen, in place of the message's right-click menu — Find all like
 * this and Exclude these (the menu's two), Copy message and Open event. The template heads it
 * with how many of the loaded events share it.
 */
@Component({
  selector: 'app-message-action-sheet',
  imports: [LucideAngularModule, BottomSheetComponent],
  templateUrl: './message-action-sheet.html',
  styleUrl: './message-action-sheet.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class MessageActionSheetComponent {
  readonly event = input.required<EventDto>();
  /** The applied filter, which Exclude narrows. */
  readonly currentFilter = input<string>('');
  /** How many loaded events share the template — the number in the heading. */
  readonly likeCount = input<number>(0);

  readonly filterSelected = output<string>();
  readonly openRequested = output<void>();
  readonly closed = output<void>();

  private readonly sanitizer = inject(DomSanitizer);

  readonly templateHtml = computed(() =>
    this.sanitizer.bypassSecurityTrustHtml(renderTemplateHtml(this.event()['@mt'])));

  readonly heading = computed(() => {
    const n = this.likeCount();
    return `Message template · ${n} ${n === 1 ? 'event' : 'events'} like this`;
  });

  readonly findCaption = computed(() => shorten(findLikeFilter(this.event()['@mt'])));
  readonly excludeCaption = computed(() =>
    shorten(`${this.currentFilter().trim() ? '… and ' : ''}@mt <> ${quoted(this.event()['@mt'] ?? '')}`));

  find(): void {
    this.filterSelected.emit(findLikeFilter(this.event()['@mt']));
  }

  exclude(): void {
    this.filterSelected.emit(excludeLikeFilter(this.event()['@mt'], this.currentFilter()));
  }

  /** The rendered message as text, as the detail's Copy message copies it. */
  async copyMessage(): Promise<void> {
    const tmp = document.createElement('div');
    tmp.innerHTML = renderMessageHtml(this.event()['@mt'], this.event().props);
    await copyToClipboard(tmp.textContent ?? '');
    this.closed.emit();
  }
}
