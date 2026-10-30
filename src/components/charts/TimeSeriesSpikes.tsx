import React from 'react';
import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts';
import { ChartCard } from './ChartCard';

/** One aggregated day (or peak bucket) of anomaly telemetry. */
export interface DailyAnomalyPoint {
  date: string;
  max_anomaly_score: number;
  avg_anomaly_score: number;
  anomaly_count: number;
}

/**
 * Marker discipline for large datasets: individual dots would fuse into a
 * solid wall, so only high-confidence peaks (score > 0.75) get a marker.
 */
const spikeDot = (props: { cx?: number; cy?: number; payload?: DailyAnomalyPoint; index?: number }) => {
  const { cx, cy, payload, index } = props;
  if (payload && payload.max_anomaly_score > 0.75 && cx != null && cy != null) {
    return <circle key={`spike-${index}`} cx={cx} cy={cy} r={3} fill="#800020" />;
  }
  return <g key={`spike-${index}`} />;
};

export const TimeSeriesSpikes: React.FC<{ data: DailyAnomalyPoint[] }> = ({ data }) => {
  return (
    <ChartCard
      title="Temporal Anomaly Spikes"
      helpText="Daily anomaly trend: the solid line follows each day's peak anomaly score, the dashed line the day's average. Markers appear only on high-confidence spikes (score above 0.75). Very long histories are trimmed to the 100 highest-peak days."
    >
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 20 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis dataKey="date" tick={{ fontSize: 10 }} minTickGap={24} label={{ value: 'Date', position: 'insideBottom', offset: -10 }} />
          <YAxis tick={{ fontSize: 10 }} domain={[0, 1]} label={{ value: 'Anomaly Score', angle: -90, position: 'insideLeft' }} />
          <Tooltip
            contentStyle={{ fontSize: '12px', borderRadius: '8px' }}
            formatter={(value: number, name: string): [string, string] => [
              Number(value).toFixed(3),
              name === 'max_anomaly_score' ? 'Peak score' : 'Average score',
            ]}
            labelFormatter={(label: string, payload) => {
              const count = payload?.[0]?.payload?.anomaly_count;
              return count != null ? `${label} — ${count} high-risk txns` : label;
            }}
          />
          <Line
            type="monotone"
            dataKey="max_anomaly_score"
            stroke="#800020"
            strokeWidth={2}
            dot={spikeDot}
            activeDot={{ r: 5 }}
            isAnimationActive={false}
          />
          <Line
            type="monotone"
            dataKey="avg_anomaly_score"
            stroke="#D97706"
            strokeWidth={1}
            strokeDasharray="4 2"
            dot={false}
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </ChartCard>
  );
};
