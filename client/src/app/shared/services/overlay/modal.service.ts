import { inject, Injectable, Injector } from '@angular/core';
import { Overlay } from '@angular/cdk/overlay';
import { ComponentType } from '@angular/cdk/portal';

import { attachComponent } from './overlay-core';
import { OverlayPanelRef } from './overlay-ref';

export interface ModalConfig<D = unknown> {
  /** Data provided to the modal component (inject `OVERLAY_DATA` or `OverlayPanelRef.data`). */
  data?: D;
  width?: string | number;
  height?: string | number;
  maxWidth?: string | number;
  panelClass?: string | string[];
  /** Render a blocking backdrop behind the modal. Defaults to `true`. */
  hasBackdrop?: boolean;
  backdropClass?: string | string[];
  /** Close when the backdrop is clicked. Defaults to `true`. */
  backdropClickClose?: boolean;
  /** Close on the Escape key. Defaults to `true`. */
  closeOnEscape?: boolean;
}

/** Opens components as centered modal dialogs over a blocking backdrop. */
@Injectable({ providedIn: 'root' })
export class ModalService {
  private readonly overlay = inject(Overlay);
  private readonly injector = inject(Injector);

  open<T, R = unknown, D = unknown>(
    component: ComponentType<T>,
    config: ModalConfig<D> = {},
  ): OverlayPanelRef<R, D> {
    const overlayRef = this.overlay.create({
      hasBackdrop: config.hasBackdrop ?? true,
      backdropClass: config.backdropClass ?? 'cdk-overlay-dark-backdrop',
      panelClass: config.panelClass ?? 'app-modal-panel',
      width: config.width,
      height: config.height,
      maxWidth: config.maxWidth ?? '90vw',
      scrollStrategy: this.overlay.scrollStrategies.block(),
      positionStrategy: this.overlay
        .position()
        .global()
        .centerHorizontally()
        .centerVertically(),
    });

    const ref = attachComponent<T, R, D>(overlayRef, component, this.injector, config.data as D);

    if (config.backdropClickClose ?? true) {
      overlayRef.backdropClick().subscribe(() => ref.close());
    }
    if (config.closeOnEscape ?? true) {
      overlayRef.keydownEvents().subscribe((e) => {
        if (e.key === 'Escape') {
          e.preventDefault();
          ref.close();
        }
      });
    }
    return ref;
  }
}
