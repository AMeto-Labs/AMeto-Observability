/** How long a finger rests before the press is a long press — the platforms' own 500 ms. */
export const LONG_PRESS_MS = 500;
/** A finger that moves further than this is scrolling, not pressing. */
export const LONG_PRESS_SLOP_PX = 8;

type Schedule = (cb: () => void, ms: number) => unknown;
type Unschedule = (handle: unknown) => void;

/**
 * A long press read off pointer events, for a touch screen's rows: `start` on pointerdown,
 * `move` on pointermove, `end` on pointerup / pointercancel. The press fires once the finger
 * has rested {@link LONG_PRESS_MS} without travelling more than {@link LONG_PRESS_SLOP_PX}.
 *
 * The click a release produces afterwards must not be read as a tap: {@link consumeFired}
 * answers whether the press that just ended had fired, and resets.
 *
 * The timer functions are parameters so a spec can drive the clock by hand — faking the real
 * timers wholesale is a trap in a zoneless app, whose change detection runs on them.
 */
export class LongPress {
  private handle: unknown;
  private x = 0;
  private y = 0;
  private fired = false;

  constructor(
    private readonly onFire: () => void,
    private readonly schedule: Schedule = (cb, ms) => setTimeout(cb, ms),
    private readonly unschedule: Unschedule = h => clearTimeout(h as ReturnType<typeof setTimeout>),
  ) {}

  start(x: number, y: number): void {
    this.cancel();
    this.fired = false;
    this.x = x;
    this.y = y;
    this.handle = this.schedule(() => {
      this.handle = undefined;
      this.fired = true;
      this.onFire();
    }, LONG_PRESS_MS);
  }

  move(x: number, y: number): void {
    if (this.handle === undefined) return;
    if (Math.abs(x - this.x) > LONG_PRESS_SLOP_PX || Math.abs(y - this.y) > LONG_PRESS_SLOP_PX) this.cancel();
  }

  /** The finger lifted, or the browser took the pointer (a scroll): a press still pending is off. */
  end(): void {
    this.cancel();
  }

  /** Whether the last press fired — true once, for the click that follows the release. */
  consumeFired(): boolean {
    const f = this.fired;
    this.fired = false;
    return f;
  }

  private cancel(): void {
    if (this.handle === undefined) return;
    this.unschedule(this.handle);
    this.handle = undefined;
  }
}
