import React from 'react';
import { ResponsiveContainer, PieChart, Pie, Tooltip, Cell, Legend } from 'recharts';
import { VendorRisk } from '../../lib/types';
import { ChartCard } from './ChartCard';

const COLORS = ['#800020', '#D97706', '#4F46E5', '#10B981', '#F59E0B', '#6B7280'];

// Belt-and-braces: the dashboard caps data to Top 5 + "Other High-Risk
// Vendors" upstream, so the legend never exceeds this. If an uncapped set
// ever reaches this component, the raw legend is hidden instead of letting
// dozens of entries spill out of the card.
const MAX_LEGEND_ITEMS = 6;

export const VendorRiskConcentration: React.FC<{ data: VendorRisk[] }> = ({ data }) => {
  const showLegend = data.length <= MAX_LEGEND_ITEMS;
  return (
    <ChartCard
      title="High-Risk Vendor Concentration"
      helpText="The five vendors carrying the highest fraud probability, with every remaining vendor grouped into a single Other slice. A large slice means that vendor is triggering a disproportionate share of anomalies."
    >
      <ResponsiveContainer width="100%" height="100%">
        <PieChart margin={{ top: 0, right: 0, left: 0, bottom: 0 }}>
          <Pie
            data={data}
            cx="50%"
            cy="42%"
            innerRadius={36}
            outerRadius={62}
            paddingAngle={2}
            dataKey="fraud_probability"
            nameKey="vendor_name"
            isAnimationActive={false}
          >
            {data.map((_entry, index) => (
              <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
            ))}
          </Pie>
          <Tooltip contentStyle={{ fontSize: '12px' }} formatter={(value: number) => `${(value * 100).toFixed(1)}%`} />
          {showLegend && (
            <Legend
              verticalAlign="bottom"
              height={44}
              formatter={(value: string) =>
                value.length > 16 ? `${value.slice(0, 16)}...` : value
              }
              wrapperStyle={{
                fontSize: '10px',
                maxHeight: 44,
                overflowY: 'auto',
                width: '100%',
              }}
            />
          )}
        </PieChart>
      </ResponsiveContainer>
    </ChartCard>
  );
};
