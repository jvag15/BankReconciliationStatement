import React from 'react';
import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts';
import { CumulativeRiskPoint } from '../../lib/types';
import { formatCompactINR } from '../../lib/utils/format';
import { ChartCard } from './ChartCard';

export const CumulativeRiskExposure: React.FC<{ data: CumulativeRiskPoint[] }> = ({ data }) => {
  return (
    <ChartCard
      title="Cumulative Financial Risk Exposure"
      helpText="This chart calculates the running total (in Rupees) of all transactions flagged as Tier 1 or Tier 2. It shows how much total capital is potentially exposed to anomalous activity over time."
    >
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={data} margin={{ top: 25, right: 20, left: 20, bottom: 25 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis
            dataKey="date"
            tick={{ fontSize: 10 }}
            interval="preserveStartEnd"
            minTickGap={32}
            label={{ value: 'Date', position: 'insideBottom', offset: -10, fontSize: 11 }}
          />
          <YAxis
            width={70}
            tick={{ fontSize: 10 }}
            tickFormatter={formatCompactINR}
            // Baseline at zero, but never clip a net-negative exposure curve
            // (amounts are signed, so the running total can dip below zero).
            domain={[(dataMin: number) => Math.min(0, dataMin), 'auto']}
            label={{ value: 'Exposure (₹)', angle: -90, position: 'insideLeft', fontSize: 11 }}
          />
          <Tooltip
            contentStyle={{ fontSize: '12px' }}
            formatter={(value: number) => [formatCompactINR(value), 'Cumulative Exposure']}
          />
          <Area
            type="step"
            dataKey="cumulative_exposure"
            stroke="#B91C1C"
            fill="#FCA5A5"
            fillOpacity={0.6}
            isAnimationActive={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </ChartCard>
  );
};
