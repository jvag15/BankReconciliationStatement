import React, { useState } from 'react';
import { HelpCircle } from 'lucide-react';

interface Props {
  title: string;
  helpText: string;
  children: React.ReactNode;
}

export const ChartCard: React.FC<Props> = ({ title, helpText, children }) => {
  const [showHelp, setShowHelp] = useState(false);

  return (
    // Fixed height + overflow-hidden hardening is scoped here (not in the
    // shared .chart-card class) so ingest/reconcile cards keep flexible
    // sizing; a chart can never bleed across its card boundary.
    <div className="chart-card h-[320px] justify-between overflow-hidden">
      <div className="chart-card-header">
        <h3 className="chart-card-title">{title}</h3>
        <button 
          onClick={() => setShowHelp(!showHelp)}
          className="text-gray-400 hover:text-vv-burgundy transition-colors relative"
          title="What does this mean?"
        >
          <HelpCircle size={16} />
        </button>
      </div>
      
      {showHelp && (
        <div className="absolute top-12 right-4 w-64 bg-blue-50 border border-blue-200 shadow-lg rounded p-3 z-10 text-xs text-blue-900 leading-relaxed">
          {helpText}
        </div>
      )}
      
      <div className="chart-container relative w-full h-full min-h-0 min-w-0 overflow-hidden">
        {children}
      </div>
    </div>
  );
};
