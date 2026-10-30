import React from 'react';
import { ResponsiveContainer, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Radar, Tooltip, Legend } from 'recharts';
import { AlgorithmAgreementMatrix } from '../../lib/types';
import { ChartCard } from './ChartCard';

export const AlgorithmAgreementsMatrix: React.FC<{ data: AlgorithmAgreementMatrix }> = ({ data }) => {
  // Convert nested matrix object to array for Recharts Radar
  const chartData = [
    {
      subject: 'vs Isolation Forest',
      DBSCAN: data.dbscan?.isolation_forest || 0,
      Statistical: data.statistical?.isolation_forest || 0
    },
    {
      subject: 'vs DBSCAN',
      IsolationForest: data.isolation_forest?.dbscan || 0,
      Statistical: data.statistical?.dbscan || 0
    },
    {
      subject: 'vs Statistical (Z)',
      IsolationForest: data.isolation_forest?.statistical || 0,
      DBSCAN: data.dbscan?.statistical || 0
    }
  ];

  return (
    <ChartCard 
      title="Algorithm Consensus Matrix" 
      helpText="Displays how often the three ML engines (Isolation Forest, DBSCAN, Statistical Z-Score) agree on whether a transaction is an anomaly. High consensus increases our confidence in the risk flags."
    >
      <ResponsiveContainer width="100%" height="100%">
        <RadarChart cx="50%" cy="50%" outerRadius="60%" data={chartData} margin={{ top: 10, right: 10, left: 10, bottom: 10 }}>
          <PolarGrid />
          <PolarAngleAxis dataKey="subject" tick={{ fontSize: 10 }} />
          <PolarRadiusAxis angle={30} domain={[0, 1]} tick={{ fontSize: 8 }} />
          <Tooltip contentStyle={{ fontSize: '12px' }} />
          <Legend wrapperStyle={{ fontSize: '10px', paddingTop: '10px' }} />
          <Radar name="Isolation Forest" dataKey="IsolationForest" stroke="#800020" fill="#800020" fillOpacity={0.4} />
          <Radar name="DBSCAN" dataKey="DBSCAN" stroke="#4F46E5" fill="#4F46E5" fillOpacity={0.4} />
          <Radar name="Statistical" dataKey="Statistical" stroke="#D97706" fill="#D97706" fillOpacity={0.4} />
        </RadarChart>
      </ResponsiveContainer>
    </ChartCard>
  );
};
