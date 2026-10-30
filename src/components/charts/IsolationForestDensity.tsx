import React from 'react';
import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip } from 'recharts';
import { IsolationForestPoint } from '../../lib/types';
import { ChartCard } from './ChartCard';

export const IsolationForestDensity: React.FC<{ data: IsolationForestPoint[] }> = ({ data }) => {
  return (
    <ChartCard 
      title="Isolation Forest Density" 
      helpText="The Isolation Forest algorithm measures how easy it is to isolate a transaction. High risk scores on the right side indicate transactions that are extremely unique compared to all others."
    >
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 20 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis dataKey="score_bin" tick={{fontSize: 10}} label={{ value: 'Risk Score Bin', position: 'insideBottom', offset: -10 }} />
          <YAxis tick={{fontSize: 10}} label={{ value: 'Count', angle: -90, position: 'insideLeft' }} />
          <Tooltip cursor={{fill: '#F9FAFB'}} contentStyle={{ fontSize: '12px' }} />
          <Bar dataKey="count" fill="#D97706" radius={[4, 4, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </ChartCard>
  );
};
