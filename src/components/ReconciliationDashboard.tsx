import { useCallback, useEffect, useState } from 'react';
import { RefreshCw, Scale } from 'lucide-react';

interface TriageRow {
  id: number;
  date: string | null;
  amount: number;
  description_raw: string | null;
  verified_ledger_category: string | null;
  confidence_score: number | null;
}

const MAX_VISIBLE_ROWS = 200;

export function ReconciliationDashboard() {
  const [rows, setRows] = useState<TriageRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchTriage = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch('/brs/triage', {
        headers: { 'X-API-Token': import.meta.env.VITE_API_AUTH_TOKEN || 'dev-token' },
      });
      if (!res.ok) {
        const body = await res.json().catch(() => null);
        setError(body?.detail || `Triage fetch failed (HTTP ${res.status}).`);
        return;
      }
      setRows((await res.json()) as TriageRow[]);
    } catch {
      setError('The reconciliation API is unreachable.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchTriage();
  }, [fetchTriage]);

  const categorized = rows.filter((r) => r.verified_ledger_category).length;
  const visible = rows.slice(0, MAX_VISIBLE_ROWS);

  return (
    <div className="gst-panel h-full">
      <div className="flex flex-col gap-6">
        <div className="flex items-end justify-between">
          <div>
            <h2 className="font-serif text-3xl font-bold text-gray-800">Reconciliation Engine</h2>
            <p className="text-sm text-gray-500 mt-1">
              Bank transactions and their current match state. Matching runs automatically as
              ledgers are ingested; refresh to see progress.
            </p>
          </div>
          <button
            onClick={fetchTriage}
            disabled={loading}
            className="flex items-center gap-2 px-3 py-2 bg-white border border-gray-300 rounded text-sm font-medium text-gray-700 hover:border-vv-burgundy hover:text-vv-burgundy transition-colors disabled:opacity-50"
          >
            <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
            {loading ? 'Refreshing…' : 'Refresh'}
          </button>
        </div>

        <div className="grid grid-cols-3 gap-4">
          <div className="chart-card">
            <p className="text-xs text-gray-500 uppercase tracking-wide">Transactions</p>
            <p className="text-2xl font-serif font-bold text-gray-800">{rows.length}</p>
          </div>
          <div className="chart-card">
            <p className="text-xs text-gray-500 uppercase tracking-wide">Categorized</p>
            <p className="text-2xl font-serif font-bold text-green-700">{categorized}</p>
          </div>
          <div className="chart-card">
            <p className="text-xs text-gray-500 uppercase tracking-wide">Awaiting category</p>
            <p className="text-2xl font-serif font-bold text-amber-600">{rows.length - categorized}</p>
          </div>
        </div>

        {error && (
          <div className="chart-card border-amber-300 bg-amber-50">
            <p className="text-sm text-amber-800">{error}</p>
          </div>
        )}

        <div className="chart-card p-0 overflow-hidden">
          <div className="chart-card-header px-4 pt-4">
            <span className="chart-card-title">
              <Scale size={15} className="text-vv-burgundy" />
              Triage queue
              {rows.length > MAX_VISIBLE_ROWS && (
                <span className="text-xs font-sans font-normal text-gray-400">
                  (showing first {MAX_VISIBLE_ROWS} of {rows.length})
                </span>
              )}
            </span>
          </div>
          <div className="overflow-x-auto max-h-[55vh] overflow-y-auto">
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-white">
                <tr className="text-left text-xs text-gray-500 uppercase tracking-wide border-b border-gray-200">
                  <th className="px-4 py-2">ID</th>
                  <th className="px-4 py-2">Date</th>
                  <th className="px-4 py-2">Description</th>
                  <th className="px-4 py-2 text-right">Amount</th>
                  <th className="px-4 py-2">Category</th>
                  <th className="px-4 py-2 text-right">Confidence</th>
                </tr>
              </thead>
              <tbody>
                {visible.map((r) => (
                  <tr key={r.id} className="border-b border-gray-100 hover:bg-vv-cream/60">
                    <td className="px-4 py-2 font-mono text-xs text-gray-500">{r.id}</td>
                    <td className="px-4 py-2 whitespace-nowrap">{r.date ?? '—'}</td>
                    <td className="px-4 py-2 max-w-[360px] truncate" title={r.description_raw ?? ''}>
                      {r.description_raw ?? '—'}
                    </td>
                    <td className="px-4 py-2 text-right font-mono">
                      ₹{Number(r.amount).toLocaleString('en-IN', { maximumFractionDigits: 2 })}
                    </td>
                    <td className="px-4 py-2">
                      {r.verified_ledger_category ? (
                        <span className="inline-block px-2 py-0.5 rounded-full text-xs bg-green-50 text-green-800 border border-green-200">
                          {r.verified_ledger_category}
                        </span>
                      ) : (
                        <span className="inline-block px-2 py-0.5 rounded-full text-xs bg-gray-50 text-gray-500 border border-gray-200">
                          uncategorized
                        </span>
                      )}
                    </td>
                    <td className="px-4 py-2 text-right font-mono text-xs">
                      {r.confidence_score != null ? r.confidence_score.toFixed(2) : '—'}
                    </td>
                  </tr>
                ))}
                {!loading && visible.length === 0 && (
                  <tr>
                    <td colSpan={6} className="px-4 py-8 text-center text-sm text-gray-400">
                      No transactions ingested yet. Upload a statement to begin.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
