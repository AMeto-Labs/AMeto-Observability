import { InjectionToken } from '@angular/core';
import type { OverlayRef } from '@angular/cdk/overlay';

/** Data object passed to the overlay content, injectable via {@link OVERLAY_DATA}. */
export const OVERLAY_DATA = new InjectionToken<unknown>('OVERLAY_DATA');

/**
 * Handle to an open overlay panel — the single contract shared by the modal,
 * dropdown and context-menu services.
 *
 * The content component (or the caller) closes the panel via {@link close},
 * optionally passing a result; {@link closed} resolves with that result once
 * the overlay is torn down. Content components inject this ref directly and
 * read {@link data} instead of receiving `@Input()`s.
 */
export class OverlayPanelRef<R = unknown, D = unknown> {
  /** Resolves with the value passed to {@link close}, or `undefined` if dismissed. */
  readonly closed: Promise<R | undefined>;

  private settle!: (result: R | undefined) => void;
  private result: R | undefined;
  private disposed = false;

  constructor(
    private readonly overlayRef: OverlayRef,
    /** The data handed to {@link open}; also provided as {@link OVERLAY_DATA}. */
    readonly data: D,
  ) {
    this.closed = new Promise<R | undefined>((resolve) => (this.settle = resolve));
    // Any teardown path (close(), backdrop, escape, service disposal) ends here.
    const sub = overlayRef.detachments().subscribe(() => {
      sub.unsubscribe();
      this.settle(this.result);
    });
  }

  /** Closes the panel and resolves {@link closed} with `result`. Idempotent. */
  close(result?: R): void {
    if (this.disposed) return;
    this.disposed = true;
    this.result = result;
    this.overlayRef.dispose();
  }
}
