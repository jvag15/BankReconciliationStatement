import React, { useState } from 'react';
import { RiskTransaction } from '../lib/types';
import { HelpCircle, Check, AlertTriangle, ShieldAlert, List } from 'lucide-react';

interface Props {
  initialData: RiskTransaction[];
}

const CATEGORIES = [
  'Salary Accounts',
  'Vendor Clearings',
  'Rent & Lease Obligations',
  'Bank Fees & Levies',
  'Utility Operations',
  'Tax Clearings/GST',
  'Office Contingencies',
  'Unclassified / Manual Review'
];

export const TransactionRiskGrid: React.FC<Props> = ({ initialData }) => {
  const [data, setData] = useState<RiskTransaction[]>(initialData);
  const [showHelp, setShowHelp] = useState(false);
  const [showAll, setShowAll] = useState(false);
  const [toastMessage, setToastMessage] = useState<string | null>(null);

  // Filter queue based on view mode
  const displayQueue = showAll ? data : data.filter(tx => tx.confidence_score >= 0.50);

  const [removingIds, setRemovingIds] = useState<Set<number>>(new Set());

  const executeRemoval = (id: number, msg: string) => {
    setRemovingIds(prev => {
      const next = new Set(prev);
      next.add(id);
      return next;
    });
    showToast(msg);
    setTimeout(() => {
      setData(prev => prev.filter(tx => tx.id !== id));
      setRemovingIds(prev => {
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
    }, 400); // Wait for the fadeOutCollapse animation to finish
  };

  const handleApprove = (id: number) => {
    executeRemoval(id, "Transaction marked as safe");
  };

  const handleOverride = (id: number, category: string) => {
    executeRemoval(id, `Transaction assigned to ${category}`);
  };

  const showToast = (msg: string) => {
    setToastMessage(msg);
    setTimeout(() => setToastMessage(null), 3000);
  };

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden relative font-sans w-full my-8">
      {/* Toast Notification */}
      {toastMessage && (
        <div className="absolute top-4 left-1/2 transform -translate-x-1/2 bg-green-800 text-white px-4 py-2 rounded shadow-lg z-50 text-sm font-medium transition-opacity">
          {toastMessage}
        </div>
      )}

      {/* Header & Help Toggle */}
      <div className="p-4 border-b border-gray-100 flex justify-between items-center bg-gray-50">
        <div>
          <h2 className="text-lg font-serif font-bold text-gray-800">Transaction Triage Queue</h2>
          <p className="text-xs text-gray-500 mt-1">Review flagged anomalies and overrides.</p>
        </div>
        <div className="flex items-center gap-4">
          <button 
            onClick={() => setShowAll(!showAll)}
            className="flex items-center gap-1 text-sm text-gray-600 hover:text-vv-burgundy font-medium"
          >
            <List size={16} />
            {showAll ? 'Show Triage Only' : 'View All Log'}
          </button>
          <button 
            onClick={() => setShowHelp(!showHelp)}
            className="flex items-center gap-1 text-sm text-gray-600 hover:text-vv-burgundy font-medium"
          >
            <HelpCircle size={16} />
            {showHelp ? 'Hide Legend' : 'Help & Legend'}
          </button>
        </div>
      </div>

      {/* Plain English Help Legend */}
      {showHelp && (
        <div className="bg-blue-50 p-4 border-b border-blue-100 text-sm text-blue-900 grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="flex gap-3 items-start">
            <ShieldAlert size={20} className="text-vv-burgundy shrink-0 mt-0.5" />
            <div>
              <strong className="block text-vv-burgundy">Tier 1: High Risk</strong>
              <span className="text-xs opacity-90">Score 85-100. Critical anomaly. Requires immediate attention.</span>
            </div>
          </div>
          <div className="flex gap-3 items-start">
            <AlertTriangle size={20} className="text-amber-600 shrink-0 mt-0.5" />
            <div>
              <strong className="block text-amber-600">Tier 2: Medium Risk</strong>
              <span className="text-xs opacity-90">Score 50-84. Unusual pattern. Review recommended.</span>
            </div>
          </div>
          <div className="flex gap-3 items-start">
            <Check size={20} className="text-gray-400 shrink-0 mt-0.5" />
            <div>
              <strong className="block text-gray-600">Tier 3: Standard (Hidden)</strong>
              <span className="text-xs opacity-90">Score 0-49. Normal activity. Safely auto-committed.</span>
            </div>
          </div>
        </div>
      )}

      {/* Triage Data Grid */}
      <div className="overflow-x-auto">
        {displayQueue.length === 0 ? (
          <div className="p-12 text-center text-green-700 font-medium flex flex-col items-center">
            <Check size={32} className="mb-2 opacity-50" />
            Inbox Zero: All flagged transactions have been reviewed.
          </div>
        ) : (
          <table className="w-full text-left text-sm whitespace-nowrap">
            <thead className="bg-gray-50 text-gray-600 font-medium border-b border-gray-200">
              <tr>
                <th className="px-6 py-3">ID</th>
                <th className="px-6 py-3">Date</th>
                <th className="px-6 py-3">Transaction Details</th>
                <th className="px-6 py-3">Amount</th>
                <th className="px-6 py-3">Risk Score</th>
                <th className="px-6 py-3">Action Controls</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {displayQueue.map(tx => {
                const isTier1 = tx.confidence_score >= 0.85;
                const isTier3 = tx.confidence_score < 0.50;
                
                let riskLabel = isTier1 ? "High Risk / Critical Anomaly" : "Review Recommended";
                let riskColor = isTier1 ? "bg-vv-burgundy text-white" : "bg-amber-100 text-amber-800";
                
                if (isTier3) {
                  riskLabel = "Standard / Auto-Committed";
                  riskColor = "bg-gray-100 text-gray-500";
                }
                
                let rowClass = isTier3 ? "bg-gray-50 opacity-75 grayscale" : "hover:bg-gray-50 transition-colors";
                if (removingIds.has(tx.id)) {
                  rowClass += " fade-out-row";
                }
                
                return (
                  <tr key={tx.id} className={rowClass}>
                    <td className="px-6 py-4 font-mono text-gray-500">{tx.id}</td>
                    <td className="px-6 py-4">{tx.date}</td>
                    <td className="px-6 py-4 truncate max-w-xs" title={tx.description_raw}>
                      {tx.description_raw}
                    </td>
                    <td className="px-6 py-4 font-mono font-medium">₹{tx.amount.toFixed(2)}</td>
                    <td className="px-6 py-4">
                      <div className="flex flex-col gap-1">
                        <span className={`px-2 py-1 rounded text-xs font-bold inline-block w-fit ${riskColor}`}>
                          {riskLabel}
                        </span>
                        <span className="text-xs text-gray-500 font-mono">
                          Score: {(tx.confidence_score * 100).toFixed(1)}
                        </span>
                      </div>
                    </td>
                    <td className="px-6 py-4">
                      <div className="flex items-center gap-2">
                        <button 
                          onClick={() => handleApprove(tx.id)}
                          disabled={isTier3}
                          className="px-3 py-1.5 bg-green-50 text-green-700 hover:bg-green-100 border border-green-200 rounded text-xs font-semibold flex items-center gap-1 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                          <Check size={14} /> Approve (Safe)
                        </button>
                        <OverrideControl id={tx.id} onOverride={handleOverride} disabled={isTier3} />
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
};

const OverrideControl = ({ id, onOverride, disabled }: { id: number, onOverride: (id: number, cat: string) => void, disabled?: boolean }) => {
  const [selected, setSelected] = useState(CATEGORIES[0]);
  
  return (
    <div className="flex items-center gap-1">
      <select 
        value={selected}
        onChange={e => setSelected(e.target.value)}
        disabled={disabled}
        className="px-2 py-1.5 border border-gray-300 rounded-l text-xs outline-none focus:border-vv-burgundy disabled:opacity-50 disabled:cursor-not-allowed"
      >
        {CATEGORIES.map(c => <option key={c} value={c}>{c}</option>)}
      </select>
      <button 
        onClick={() => onOverride(id, selected)}
        disabled={disabled}
        className="px-3 py-1.5 bg-gray-100 text-gray-700 hover:bg-gray-200 border border-l-0 border-gray-300 rounded-r text-xs font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
      >
        Commit
      </button>
    </div>
  );
};
