import React from 'react';
import { ResponsiveContainer, ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, Cell } from 'recharts';
import { DBSCANPoint } from '../../lib/types';
import { ChartCard } from './ChartCard';

import { formatCompactINR } from '../../lib/utils/format';

export const DBSCANScatter: React.FC<{ data: DBSCANPoint[] }> = ({ data }) => {
  return (
    <ChartCard
      title="DBSCAN Spatial Clusters"
      helpText="DBSCAN groups mathematically similar transactions together. Gray dots are a representative sample of normal cluster members; red dots are every outlier that does not belong to any known pattern."
    >
      <ResponsiveContainer width="100%" height="100%">
        <ScatterChart margin={{ top: 10, right: 10, left: 8, bottom: 20 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis
            type="number"
            dataKey="description_length"
            name="Text Length"
            tick={{ fontSize: 10 }}
            label={{ value: 'Description Length', position: 'insideBottom', offset: -10 }}
          />
          <YAxis
            type="number"
            dataKey="abs_amount"
            name="Amount"
            width={52}
            tick={{ fontSize: 10 }}
            tickFormatter={formatCompactINR}
            label={{ value: 'Amount', angle: -90, position: 'insideLeft' }}
          />
          <Tooltip
            cursor={{ strokeDasharray: '3 3' }}
            contentStyle={{ fontSize: '12px' }}
            formatter={(value: number, name: string) =>
              name === 'Amount' ? formatCompactINR(value) : value
            }
          />
          <Scatter name="Transactions" data={data} isAnimationActive={false}>
            {data.map((entry, index) => (
              <Cell
                key={`cell-${index}`}
                fill={entry.is_anomaly ? '#800020' : '#9CA3AF'}
                fillOpacity={entry.is_anomaly ? 0.9 : 0.5}
              />
            ))}
          </Scatter>
        </ScatterChart>
      </ResponsiveContainer>
    </ChartCard>
  );
};
