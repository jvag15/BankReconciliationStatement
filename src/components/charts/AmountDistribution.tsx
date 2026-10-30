import React from 'react';
import { ResponsiveContainer, ComposedChart, Area, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts';
import { AmountDistributionPoint } from '../../lib/types';
import { formatCompactINR, formatCompactCount } from '../../lib/utils/format';
import { ChartCard } from './ChartCard';

/** One uniform histogram bucket of transaction amounts. */
export interface AmountBin {
  bin_start: number;
  bin_end: number;
  base_count: number;
  anomaly_count: number;
}

const BIN_COUNT = 12;

/** Bin raw per-transaction points into uniform amount buckets.

 * Raw points plot one X entry per transaction, which at 25k rows renders
 * dozens of colliding axis labels; a fixed-bucket histogram keeps the axis
 * to BIN_COUNT ticks regardless of dataset size.
 */
export function binAmountDistribution(points: AmountDistributionPoint[]): AmountBin[] {
  if (points.length === 0) return [];
  let min = Infinity;
  let max = -Infinity;
  for (const p of points) {
    if (p.amount < min) min = p.amount;
    if (p.amount > max) max = p.amount;
  }
  const width = (max - min) / BIN_COUNT || 1;
  const bins: AmountBin[] = Array.from({ length: BIN_COUNT }, (_, i) => ({
    bin_start: min + i * width,
    bin_end: min + (i + 1) * width,
    base_count: 0,
    anomaly_count: 0,
  }));
  for (const p of points) {
    const idx = Math.min(BIN_COUNT - 1, Math.floor((p.amount - min) / width));
    if (p.is_anomaly) bins[idx].anomaly_count += 1;
    else bins[idx].base_count += 1;
  }
  return bins;
}

export const AmountDistribution: React.FC<{ data: AmountBin[] }> = ({ data }) => {
  return (
    <ChartCard
      title="Transaction Amount Distribution"
      helpText="Histogram of transaction sizes in uniform amount buckets: the shaded area profiles typical volumes, the burgundy bars count anomalies per bucket. Concentration of bars at specific value ranges points to threshold-seeking behaviour."
    >
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={data} margin={{ top: 20, right: 25, left: 20, bottom: 35 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis
            dataKey="bin_start"
            tick={{ fontSize: 9 }}
            interval="preserveStartEnd"
            minTickGap={28}
            tickFormatter={(val) => formatCompactINR(Number(val))}
            label={{ value: 'Amount (₹)', position: 'insideBottom', offset: -10, fontSize: 11 }}
          />
          <YAxis
            width={65}
            tick={{ fontSize: 10 }}
            allowDecimals={false}
            tickFormatter={formatCompactCount}
            label={{ value: 'Frequency', angle: -90, position: 'insideLeft', fontSize: 11 }}
          />
          <Tooltip
            contentStyle={{ fontSize: '12px' }}
            labelFormatter={(val: number, payload) => {
              const bin = payload?.[0]?.payload as AmountBin | undefined;
              return bin
                ? `${formatCompactINR(bin.bin_start)} to ${formatCompactINR(bin.bin_end)}`
                : formatCompactINR(Number(val));
            }}
          />
          <Legend verticalAlign="bottom" wrapperStyle={{ fontSize: '10px', paddingTop: '15px' }} />
          <Area
            type="monotone"
            dataKey="base_count"
            name="Base Profile"
            fill="#4F46E5"
            stroke="#4F46E5"
            fillOpacity={0.1}
            isAnimationActive={false}
          />
          <Bar
            dataKey="anomaly_count"
            name="Anomaly Density"
            fill="#800020"
            barSize={10}
            isAnimationActive={false}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </ChartCard>
  );
};
