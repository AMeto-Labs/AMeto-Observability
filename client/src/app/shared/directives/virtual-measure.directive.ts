import { Directive, ElementRef, inject, input, OnDestroy, OnInit } from '@angular/core';

/**
 * Attaches a ResizeObserver to the host element and calls the provided
 * `measureElement` function whenever the element's size changes.
 * Used with TanStack Virtual to support dynamic row heights.
 * The host element must have [attr.data-index] set so TanStack can
 * map the measurement back to the correct virtual item.
 */
@Directive({
  selector: '[vmeasure]',
  standalone: true,
})
export class VmeasureDirective implements OnInit, OnDestroy {
  private readonly el = inject(ElementRef<Element>);
  readonly vmeasure = input.required<(el: Element) => void>();

  private ro = new ResizeObserver(() => this.vmeasure()(this.el.nativeElement));

  ngOnInit(): void {
    this.ro.observe(this.el.nativeElement);
    this.vmeasure()(this.el.nativeElement);
  }

  ngOnDestroy(): void {
    this.ro.disconnect();
  }
}
