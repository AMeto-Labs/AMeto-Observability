import type { Chart as ChartType } from 'chart.js';

export type ChartCtor = typeof ChartType;

let _chart: ChartCtor | null = null;
let _loading: Promise<ChartCtor> | null = null;

/**
 * Lazily loads Chart.js together with the zoom and matrix plugins and registers
 * everything exactly once. Chart.js (+ plugins) is ~200 KB — importing it on
 * demand keeps it out of the metrics route chunk, so the page shell/controls
 * render immediately and the charting library streams in only when a chart is
 * actually about to be drawn.
 *
 * `registerables` already covers the line chart's controllers, scales, elements
 * and the Tooltip/Legend plugins, so both the metrics line chart and the matrix
 * heatmap are satisfied by this single registration.
 */
export function loadChart(): Promise<ChartCtor> {
  if (_chart) return Promise.resolve(_chart);
  _loading ??= (async () => {
    const [{ Chart, registerables }, zoom, matrix] = await Promise.all([
      import('chart.js'),
      import('chartjs-plugin-zoom'),
      import('chartjs-chart-matrix'),
    ]);
    Chart.register(...registerables, zoom.default, matrix.MatrixController, matrix.MatrixElement);
    _chart = Chart;
    return Chart;
  })();
  return _loading;
}
