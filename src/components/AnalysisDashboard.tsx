import { useEffect, useMemo, useState } from 'react';
import { FileDown, RefreshCw, Radio, WifiOff } from 'lucide-react';
import { TransactionRiskGrid } from './TransactionRiskGrid';
import { mockAnalysisData } from '../lib/mockData';
import {
  AnalysisDataPayload,
  CumulativeRiskPoint,
  DBSCANPoint,
  RiskTransaction,
  TemporalHeatmapPoint,
  TimeSeriesPoint,
  VendorRisk,
} from '../lib/types';

// Charts
import { TimeSeriesSpikes, DailyAnomalyPoint } from './charts/TimeSeriesSpikes';
import { DBSCANScatter } from './charts/DBSCANScatter';
import { IsolationForestDensity } from './charts/IsolationForestDensity';
import { ZScoreCurve } from './charts/ZScoreCurve';
import { VendorRiskConcentration } from './charts/VendorRiskConcentration';
import { TemporalHeatmap } from './charts/TemporalHeatmap';
import { AmountDistribution, binAmountDistribution } from './charts/AmountDistribution';
import { CumulativeRiskExposure } from './charts/CumulativeRiskExposure';
import { AlgorithmAgreementsMatrix } from './charts/AlgorithmAgreementsMatrix';

// ── Large-dataset display transforms ──────────────────────────────────────
// Raw payloads scale with the transaction count (25k+ under stress); these
// reduce them to chart-sized shapes before rendering so the charts stay
// legible and the DOM stays small.

const MAX_TIMELINE_POINTS = 100;
const HIGH_ANOMALY_THRESHOLD = 0.7;
const TOP_VENDOR_SLICES = 5;
const MAX_CORE_SCATTER_POINTS = 150;

/** Group per-transaction anomaly scores into one point per day. */
function aggregateTimeSeries(points: TimeSeriesPoint[]): DailyAnomalyPoint[] {
  const byDate = new Map<string, { max: number; sum: number; n: number; high: number }>();
  for (const p of points) {
    const bucket = byDate.get(p.date) ?? { max: -Infinity, sum: 0, n: 0, high: 0 };
    bucket.max = Math.max(bucket.max, p.anomaly_score);
    bucket.sum += p.anomaly_score;
    bucket.n += 1;
    if (p.anomaly_score > HIGH_ANOMALY_THRESHOLD) bucket.high += 1;
    byDate.set(p.date, bucket);
  }
  let daily: DailyAnomalyPoint[] = [...byDate.entries()].map(([date, b]) => ({
    date,
    max_anomaly_score: Number(b.max.toFixed(4)),
    avg_anomaly_score: Number((b.sum / b.n).toFixed(4)),
    anomaly_count: b.high,
  }));
  daily.sort((a, b) => a.date.localeCompare(b.date));
  // Very long histories: keep the 100 highest-peak days, in date order.
  if (daily.length > MAX_TIMELINE_POINTS) {
    daily = [...daily]
      .sort((a, b) => b.max_anomaly_score - a.max_anomaly_score)
      .slice(0, MAX_TIMELINE_POINTS)
      .sort((a, b) => a.date.localeCompare(b.date));
  }
  return daily;
}

/** Top N highest-risk vendors as explicit slices; the rest as one bucket.

 * Vendor names are raw bank narration prefixes and can carry embedded
 * newlines/runs of whitespace, which break legend layout; collapse them.
 */
function capVendorSlices(vendors: VendorRisk[]): VendorRisk[] {
  const cleaned = vendors.map((v) => ({
    ...v,
    vendor_name: v.vendor_name.replace(/\s+/g, ' ').trim(),
  }));
  if (cleaned.length <= TOP_VENDOR_SLICES + 1) return cleaned;
  const sorted = cleaned.sort((a, b) => b.fraud_probability - a.fraud_probability);
  const top = sorted.slice(0, TOP_VENDOR_SLICES);
  const otherWeight = sorted
    .slice(TOP_VENDOR_SLICES)
    .reduce((sum, v) => sum + v.fraud_probability, 0);
  return [
    ...top,
    { vendor_name: 'Other High-Risk Vendors', fraud_probability: Number(otherWeight.toFixed(4)) },
  ];
}

/** All anomalies plus a stratified sample of core cluster points. */
function sampleScatter(points: DBSCANPoint[]): DBSCANPoint[] {
  const anomalies = points.filter((p) => p.is_anomaly || p.cluster_id === -1);
  const core = points.filter((p) => !(p.is_anomaly || p.cluster_id === -1));
  if (core.length <= MAX_CORE_SCATTER_POINTS) return [...core, ...anomalies];
  const step = core.length / MAX_CORE_SCATTER_POINTS;
  const sampled = Array.from(
    { length: MAX_CORE_SCATTER_POINTS },
    (_, i) => core[Math.floor(i * step)],
  );
  // Anomalies last so they draw on top of the baseline sample.
  return [...sampled, ...anomalies];
}

// The remaining per-transaction arrays freeze the renderer outright at 25k+
// rows (observed: an unresponsive main thread at 50k), so they are reduced
// with the same shape-preserving discipline.

const MAX_CUMULATIVE_POINTS = 300;
const MAX_RISK_GRID_ROWS = 500;

/** Collapse per-transaction heat points into day-of-week x hour cells. */
function aggregateHeatmap(points: TemporalHeatmapPoint[]): TemporalHeatmapPoint[] {
  const cells = new Map<string, { day: string; hour: number; sum: number; n: number }>();
  for (const p of points) {
    const key = `${p.day_of_week}|${p.hour}`;
    const cell = cells.get(key) ?? { day: p.day_of_week, hour: p.hour, sum: 0, n: 0 };
    cell.sum += p.risk_score;
    cell.n += 1;
    cells.set(key, cell);
  }
  return [...cells.values()].map((c) => ({
    day_of_week: c.day,
    hour: c.hour,
    risk_score: Number((c.sum / c.n).toFixed(4)),
  }));
}

/** Evenly-strided reduction that always keeps the final (total) point. */
function sampleCumulative(points: CumulativeRiskPoint[]): CumulativeRiskPoint[] {
  if (points.length <= MAX_CUMULATIVE_POINTS) return points;
  const step = points.length / (MAX_CUMULATIVE_POINTS - 1);
  const sampled = Array.from(
    { length: MAX_CUMULATIVE_POINTS - 1 },
    (_, i) => points[Math.floor(i * step)],
  );
  sampled.push(points[points.length - 1]);
  return sampled;
}

/** The triage grid shows the highest-risk rows, not fifty thousand DOM rows. */
function capRiskGrid(rows: RiskTransaction[]): RiskTransaction[] {
  if (rows.length <= MAX_RISK_GRID_ROWS) return rows;
  return [...rows]
    .sort((a, b) => b.confidence_score - a.confidence_score)
    .slice(0, MAX_RISK_GRID_ROWS);
}

export function AnalysisDashboard() {
  const [data, setData] = useState<AnalysisDataPayload>(mockAnalysisData);
  const [loading, setLoading] = useState(true);
  const [isLive, setIsLive] = useState(false);

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    setLoading(true);
    try {
      const res = await fetch('/brs/analysis', {
        headers: { 'X-API-Token': import.meta.env.VITE_API_AUTH_TOKEN || 'dev-token' }
      });
      if (res.ok) {
        const json = await res.json();
        // Check if data is populated from backend, otherwise fallback to mock
        if (json.timeSeries && json.timeSeries.length > 0) {
          setData(json);
          setIsLive(true);
        }
      }
    } catch (e) {
      console.warn("Using mock data as backend is unreachable.");
      setIsLive(false);
    } finally {
      setLoading(false);
    }
  };

  const displayTimeSeries = useMemo(() => aggregateTimeSeries(data.timeSeries), [data]);
  const displayVendors = useMemo(() => capVendorSlices(data.vendorRisk), [data]);
  const displayScatter = useMemo(() => sampleScatter(data.dbscanScatter), [data]);
  const displayHeatmap = useMemo(() => aggregateHeatmap(data.temporalHeatmap), [data]);
  const displayDistribution = useMemo(() => binAmountDistribution(data.amountDistribution), [data]);
  const displayCumulative = useMemo(() => sampleCumulative(data.cumulativeRisk), [data]);
  const displayRiskRows = useMemo(() => capRiskGrid(data.riskTransactions), [data]);

  const highRiskCount = data.riskTransactions.filter(tx => tx.confidence_score >= 0.85).length;
  const reviewCount = data.riskTransactions.filter(tx => tx.confidence_score >= 0.50 && tx.confidence_score < 0.85).length;
  const totalExposure = data.cumulativeRisk.length > 0
    ? data.cumulativeRisk[data.cumulativeRisk.length - 1].cumulative_exposure
    : 0;

  return (
    <div className="gst-workbench">
      <aside className="gst-rail p-6 flex flex-col gap-8">
        <div>
          <h2 className="font-serif text-2xl font-bold text-vv-burgundy leading-tight">Triage & Analysis</h2>
          <p className="text-xs text-gray-500 mt-1">Multi-Algorithm Triage</p>
        </div>

        <div className="flex items-center gap-2 text-xs font-medium">
          {isLive ? (
            <span className="flex items-center gap-1.5 text-green-700">
              <Radio size={14} /> Live data
            </span>
          ) : (
            <span className="flex items-center gap-1.5 text-gray-400">
              <WifiOff size={14} /> Mock data (backend unreachable)
            </span>
          )}
        </div>

        <button
          onClick={fetchData}
          disabled={loading}
          className="no-print flex items-center justify-center gap-2 px-3 py-2 bg-white border border-gray-300 rounded text-sm font-medium text-gray-700 hover:border-vv-burgundy hover:text-vv-burgundy transition-colors disabled:opacity-50"
        >
          <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
          {loading ? 'Refreshing…' : 'Refresh analysis'}
        </button>

        <button
          onClick={() => window.print()}
          className="no-print flex items-center justify-center gap-2 px-3 py-2 bg-vv-burgundy border border-vv-burgundy rounded text-sm font-medium text-white hover:opacity-90 transition-opacity"
          title="Format the dashboard as a printable PDF report"
        >
          <FileDown size={14} />
          Export PDF Report
        </button>

        <dl className="flex flex-col gap-4">
          <div className="border-t border-gray-100 pt-4">
            <dt className="text-xs text-gray-500 uppercase tracking-wide">Transactions scanned</dt>
            <dd className="text-2xl font-serif font-bold text-gray-800">{data.riskTransactions.length}</dd>
          </div>
          <div className="border-t border-gray-100 pt-4">
            <dt className="text-xs text-gray-500 uppercase tracking-wide">High risk</dt>
            <dd className="text-2xl font-serif font-bold text-vv-burgundy">{highRiskCount}</dd>
          </div>
          <div className="border-t border-gray-100 pt-4">
            <dt className="text-xs text-gray-500 uppercase tracking-wide">Needs review</dt>
            <dd className="text-2xl font-serif font-bold text-amber-600">{reviewCount}</dd>
          </div>
          <div className="border-t border-gray-100 pt-4">
            <dt className="text-xs text-gray-500 uppercase tracking-wide">Risk-weighted exposure</dt>
            <dd className="text-lg font-mono font-medium text-gray-800">
              ₹{totalExposure.toLocaleString('en-IN', { maximumFractionDigits: 0 })}
            </dd>
          </div>
        </dl>
      </aside>

      <main className="gst-panel">
        <div className="analysis-grid">
          <TimeSeriesSpikes data={displayTimeSeries} />
          <DBSCANScatter data={displayScatter} />
          <IsolationForestDensity data={data.isolationForestDensity} />

          <ZScoreCurve data={data.zScoreCurve} />
          <VendorRiskConcentration data={displayVendors} />
          <TemporalHeatmap data={displayHeatmap} />

          <AmountDistribution data={displayDistribution} />
          <CumulativeRiskExposure data={displayCumulative} />
          <AlgorithmAgreementsMatrix data={data.algorithmAgreement} />
        </div>

        <TransactionRiskGrid initialData={displayRiskRows} />
      </main>
    </div>
  );
}
