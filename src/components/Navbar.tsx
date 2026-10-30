import { useState } from 'react';
import { Upload, Scale, Activity, RotateCcw, Loader2 } from 'lucide-react';

export type WorkbenchTab = 'ingest' | 'reconcile' | 'analysis';

interface NavbarProps {
  activeTab: WorkbenchTab;
  setActiveTab: (tab: WorkbenchTab) => void;
}

const TABS: { id: WorkbenchTab; label: string; icon: typeof Upload }[] = [
  { id: 'ingest', label: 'Upload Statement', icon: Upload },
  { id: 'reconcile', label: 'Reconciliation Engine', icon: Scale },
  { id: 'analysis', label: 'Triage & Analysis', icon: Activity },
];

export function Navbar({ activeTab, setActiveTab }: NavbarProps) {
  const [resetting, setResetting] = useState(false);

  const clearSession = async () => {
    const confirmed = window.confirm(
      'Clear session? This permanently removes all ingested bank and ledger rows so a new job can start clean.'
    );
    if (!confirmed) return;
    setResetting(true);
    try {
      const res = await fetch('/brs/session/reset', {
        method: 'POST',
        headers: { 'X-API-Token': import.meta.env.VITE_API_AUTH_TOKEN || 'dev-token' },
      });
      if (!res.ok) {
        const body = await res.json().catch(() => null);
        window.alert(body?.detail || `Session reset failed (HTTP ${res.status}).`);
        return;
      }
      // Full reload drops every piece of local view state and lands back on
      // the clean dropzone (the default tab).
      window.location.reload();
    } catch {
      window.alert('Session reset failed: the API is unreachable.');
    } finally {
      setResetting(false);
    }
  };

  return (
    <header className="bg-white border-b border-gray-200 flex items-center justify-between px-6 shrink-0 no-print">
      <div className="py-3">
        <h1 className="font-serif text-2xl font-bold text-vv-burgundy leading-tight">
          BRS Engine
        </h1>
      </div>

      <div className="flex items-stretch self-stretch gap-4">
        <nav className="flex items-stretch gap-1">
          {TABS.map(({ id, label, icon: Icon }) => {
            const active = activeTab === id;
            return (
              <button
                key={id}
                onClick={() => setActiveTab(id)}
                className={
                  'flex items-center gap-2 px-4 text-sm font-medium border-b-2 transition-colors ' +
                  (active
                    ? 'border-vv-burgundy text-vv-burgundy bg-vv-cream'
                    : 'border-transparent text-gray-600 hover:text-vv-burgundy hover:bg-gray-50')
                }
              >
                <Icon size={15} />
                {label}
              </button>
            );
          })}
        </nav>

        <div className="flex items-center">
          <button
            onClick={clearSession}
            disabled={resetting}
            className="flex items-center gap-2 px-3 py-1.5 text-xs font-medium text-gray-600 border border-gray-300 rounded hover:border-vv-burgundy hover:text-vv-burgundy transition-colors disabled:opacity-50"
            title="Truncate all ingested data and start a fresh job"
          >
            {resetting ? <Loader2 size={13} className="animate-spin" /> : <RotateCcw size={13} />}
            {resetting ? 'Clearing…' : 'Clear Session / New Job'}
          </button>
        </div>
      </div>
    </header>
  );
}
