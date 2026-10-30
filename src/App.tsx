import { useState } from 'react';
import { Navbar, WorkbenchTab } from './components/Navbar';
import { IngestScreen } from './components/IngestScreen';
import { ReconciliationDashboard } from './components/ReconciliationDashboard';
import { AnalysisDashboard } from './components/AnalysisDashboard';

function App() {
  // Ingestion-first workflow: the app lands on the upload view.
  const [activeTab, setActiveTab] = useState<WorkbenchTab>('ingest');

  return (
    <div className="app-shell flex flex-col h-screen overflow-hidden">
      <Navbar activeTab={activeTab} setActiveTab={setActiveTab} />
      <div className="flex-1 overflow-hidden">
        {activeTab === 'ingest' && <IngestScreen setActiveTab={setActiveTab} />}
        {activeTab === 'reconcile' && <ReconciliationDashboard />}
        {activeTab === 'analysis' && <AnalysisDashboard />}
      </div>
    </div>
  );
}

export default App;
