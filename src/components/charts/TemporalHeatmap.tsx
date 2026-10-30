import React from 'react';
import { ResponsiveContainer, ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, ZAxis } from 'recharts';
import { TemporalHeatmapPoint } from '../../lib/types';
import { ChartCard } from './ChartCard';

export const TemporalHeatmap: React.FC<{ data: TemporalHeatmapPoint[] }> = ({ data }) => {
  // Recharts doesn't have a native Heatmap, so we use a ScatterChart with large square markers
  // mapping X to hour and Y to day_of_week. The ZAxis controls the size/color intensity proxy.

  return (
    <ChartCard 
      title="Temporal Risk Heatmap" 
      helpText="Maps when anomalies occur most frequently. Large dark squares indicate that a high volume of risky transactions were processed during that specific hour and day."
    >
      <ResponsiveContainer width="100%" height="100%">
        <ScatterChart margin={{ top: 10, right: 10, left: 20, bottom: 20 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis type="number" dataKey="hour" name="Hour" domain={[0, 23]} tickCount={24} tick={{fontSize: 10}} label={{ value: 'Hour of Day', position: 'insideBottom', offset: -10 }} />
          <YAxis type="category" dataKey="day_of_week" name="Day" allowDuplicatedCategory={false} tick={{fontSize: 10}} />
          <ZAxis type="number" dataKey="risk_score" range={[10, 200]} name="Avg Risk Score" />
          <Tooltip cursor={{ strokeDasharray: '3 3' }} contentStyle={{ fontSize: '12px' }} />
          <Scatter name="Risk Score" data={data} fill="#800020" shape="square" opacity={0.7} />
        </ScatterChart>
      </ResponsiveContainer>
    </ChartCard>
  );
};
