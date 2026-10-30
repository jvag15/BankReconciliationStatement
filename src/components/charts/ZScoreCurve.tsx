import React from 'react';
import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts';
import { ZScorePoint } from '../../lib/types';
import { ChartCard } from './ChartCard';

export const ZScoreCurve: React.FC<{ data: ZScorePoint[] }> = ({ data }) => {
  return (
    <ChartCard 
      title="Z-Score Deviation Curve" 
      helpText="The Z-Score shows how far an amount deviates from that specific vendor's average. A high Z-Score indicates a transaction that is significantly larger than typical payments to that vendor."
    >
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 20 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis dataKey="deviation_bin" tick={{fontSize: 10}} label={{ value: 'Z-Score (Standard Deviations)', position: 'insideBottom', offset: -10 }} />
          <YAxis tick={{fontSize: 10}} label={{ value: 'Frequency', angle: -90, position: 'insideLeft' }} />
          <Tooltip contentStyle={{ fontSize: '12px' }} />
          <Area type="monotone" dataKey="count" stroke="#4F46E5" fill="#4F46E5" fillOpacity={0.3} />
        </AreaChart>
      </ResponsiveContainer>
    </ChartCard>
  );
};
