import { useEffect, useRef, useState } from 'react';
import {
  UploadCloud,
  FileCheck2,
  AlertTriangle,
  Scale,
  Activity,
  Loader2,
} from 'lucide-react';
import type { WorkbenchTab } from './Navbar';

interface TaskReceipt {
  task_id: string;
  status: string;
  filename_assigned: string;
}

interface IngestScreenProps {
  setActiveTab: (tab: WorkbenchTab) => void;
}

type IngestStatus = 'idle' | 'uploading' | 'processing' | 'completed' | 'failed';

const ACCEPTED_EXTENSIONS = ['.csv', '.txt', '.enc'];
const COMPLETION_TIMEOUT_MS = 120_000;

const getApiToken = (): string => {
  return import.meta.env.VITE_API_AUTH_TOKEN 
    || localStorage.getItem('brs_api_token') 
    || 'dev-token';
};

export function IngestScreen({ setActiveTab }: IngestScreenProps) {
  const [dragOver, setDragOver] = useState(false);
  const [status, setStatus] = useState<IngestStatus>('idle');
  const [statusMessage, setStatusMessage] = useState('');
  const [receipt, setReceipt] = useState<TaskReceipt | null>(null);
  const [uploadedName, setUploadedName] = useState('');
  const [rowsWritten, setRowsWritten] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const streamRef = useRef<EventSource | null>(null);
  const timeoutRef = useRef<number | null>(null);

  const teardownStream = () => {
    if (streamRef.current) {
      streamRef.current.close();
      streamRef.current = null;
    }
    if (timeoutRef.current !== null) {
      window.clearTimeout(timeoutRef.current);
      timeoutRef.current = null;
    }
  };

  useEffect(() => {
    if (!localStorage.getItem('brs_api_token')) {
      const fallback = 'dev-token';
      localStorage.setItem('brs_api_token', import.meta.env.VITE_API_AUTH_TOKEN || fallback);
    }
    return teardownStream;
  }, []);

  const fail = (reason: string) => {
    teardownStream();
    setStatus('failed');
    setError(reason);
  };

  const complete = (rows: number | null) => {
    teardownStream();
    setRowsWritten(rows);
    setStatus('completed');
    setStatusMessage('Statement fully processed.');
  };

  const subscribeToProgress = async (chunkId: string) => {
    // Ephemeral single-use token: EventSource cannot set headers, and the
    // long-lived API token must never ride in a query string.
    let streamToken: string;
    try {
      const res = await fetch('/brs/auth/stream-token', {
        method: 'POST',
        headers: { 'X-API-Token': getApiToken() },
      });
      if (!res.ok) throw new Error(`mint failed (HTTP ${res.status})`);
      streamToken = (await res.json()).stream_token;
    } catch {
      fail('Could not subscribe to the processing event stream. The statement is still queued; check the Reconciliation view shortly.');
      return;
    }

    const es = new EventSource(`/brs/events/stream?token=${streamToken}`);
    streamRef.current = es;

    timeoutRef.current = window.setTimeout(() => {
      fail('Timed out waiting for the worker to finish this statement. It may still complete in the background; check the Reconciliation view.');
    }, COMPLETION_TIMEOUT_MS);

    es.onmessage = (e) => {
      let event_type = '';
      let payload: Record<string, unknown> = {};
      try {
        const parsed = JSON.parse(e.data);
        event_type = parsed.event_type;
        payload = parsed.payload ?? {};
      } catch {
        return;
      }
      if (payload.chunk_id !== chunkId) return;

      if (event_type === 'BATCH_STARTED') {
        setStatusMessage('Processing statement chunks…');
      } else if (event_type === 'BATCH_COMPLETED') {
        const rows = typeof payload.rows_written === 'number' ? payload.rows_written : null;
        complete(rows);
      } else if (event_type === 'BATCH_FAILED' || event_type === 'TASK_FAILED') {
        const reason = typeof payload.reason === 'string' ? payload.reason : 'unknown worker failure';
        fail(`The worker could not process this statement (${reason}). It has been routed to the dead-letter queue for recovery.`);
      }
    };

    // Single-use tokens make EventSource auto-reconnect impossible by
    // design; treat a dropped stream as a tracking failure per contract.
    es.onerror = () => {
      if (streamRef.current) {
        fail('The processing event stream was interrupted. The statement may still complete in the background; check the Reconciliation view.');
      }
    };
  };

  const uploadFile = async (file: File) => {
    setError(null);
    setReceipt(null);
    setRowsWritten(null);
    teardownStream();

    const extension = file.name.slice(file.name.lastIndexOf('.')).toLowerCase();
    if (!ACCEPTED_EXTENSIONS.includes(extension)) {
      setStatus('failed');
      setError(`Unsupported file type "${extension}". Accepted formats: ${ACCEPTED_EXTENSIONS.join(', ')}.`);
      return;
    }

    setStatus('uploading');
    setStatusMessage('Encrypting and queueing statement…');
    try {
      const form = new FormData();
      form.append('file', file);
      const res = await fetch('/api/v1/statements/ingest', {
        method: 'POST',
        headers: { 'X-API-Token': getApiToken() },
        body: form,
      });
      
      let body: ({ detail?: string; message?: string } & Partial<TaskReceipt>) | null = null;
      try {
        body = await res.json();
      } catch (e) {
        // Ignored, non-JSON response
      }

      if (!res.ok) {
        setStatus('failed');
        const errorMsg = body?.detail || body?.message || `Upload rejected (HTTP ${res.status}).`;
        setError(errorMsg);
        return;
      }
      
      const accepted = body as unknown as TaskReceipt;
      setReceipt(accepted);
      setUploadedName(file.name);
      setStatus('processing');
      setStatusMessage('Statement queued — waiting for a worker…');
      subscribeToProgress(accepted.filename_assigned);
    } catch {
      setStatus('failed');
      setError('Upload failed: the ingestion API is unreachable.');
    }
  };

  const onDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const file = e.dataTransfer.files?.[0];
    if (file) uploadFile(file);
  };

  const onPick = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) uploadFile(file);
    e.target.value = '';
  };

  const busy = status === 'uploading' || status === 'processing';

  return (
    <div className="gst-panel h-full">
      <div className="max-w-3xl mx-auto flex flex-col gap-6 pt-8">
        <div>
          <h2 className="font-serif text-3xl font-bold text-gray-800">Upload Statement</h2>
          <p className="text-sm text-gray-500 mt-1">
            Drop a bank statement export below. The file is encrypted at rest and parsed
            asynchronously by the processing pipeline.
          </p>
        </div>

        <div
          onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
          onDragLeave={() => setDragOver(false)}
          onDrop={onDrop}
          onClick={() => { if (!busy) inputRef.current?.click(); }}
          className={
            'chart-card items-center justify-center text-center py-16 border-2 border-dashed transition-colors ' +
            (busy
              ? 'border-gray-200 cursor-default'
              : dragOver
                ? 'border-vv-burgundy bg-vv-cream cursor-pointer'
                : 'border-gray-300 hover:border-vv-burgundy cursor-pointer')
          }
        >
          <input
            ref={inputRef}
            type="file"
            accept={ACCEPTED_EXTENSIONS.join(',')}
            className="hidden"
            onChange={onPick}
          />
          {status === 'uploading' ? (
            <div className="flex flex-col items-center gap-3 text-gray-600">
              <Loader2 size={40} className="animate-spin text-vv-burgundy" />
              <p className="text-sm font-medium">{statusMessage}</p>
            </div>
          ) : (
            <div className="flex flex-col items-center gap-3 text-gray-600">
              <UploadCloud size={40} className="text-vv-burgundy" />
              <p className="text-sm font-medium">
                Drag a statement file here, or click to browse
              </p>
              <p className="text-xs text-gray-400">
                Accepted formats: {ACCEPTED_EXTENSIONS.join(', ')}
              </p>
            </div>
          )}
        </div>

        {status === 'processing' && receipt && (
          <div className="chart-card gap-3">
            <div className="flex items-center gap-3">
              <Loader2 size={20} className="animate-spin text-vv-burgundy shrink-0" />
              <div>
                <p className="text-sm font-bold text-gray-800">{statusMessage}</p>
                <p className="text-xs text-gray-500 mt-0.5">
                  {uploadedName} — task <span className="font-mono">{receipt.task_id}</span>
                </p>
              </div>
            </div>
            <div className="h-1.5 w-full bg-gray-100 rounded-full overflow-hidden">
              <div className="h-full w-1/3 bg-vv-burgundy rounded-full animate-pulse" />
            </div>
          </div>
        )}

        {status === 'failed' && error && (
          <div className="chart-card flex-row items-start gap-3 border-amber-300 bg-amber-50">
            <AlertTriangle size={18} className="text-amber-600 mt-0.5 shrink-0" />
            <div>
              <p className="text-sm font-medium text-amber-800">Statement not processed</p>
              <p className="text-xs text-amber-700 mt-1">{error}</p>
            </div>
          </div>
        )}

        {status === 'completed' && receipt && (
          <div className="chart-card gap-4">
            <div className="flex items-start gap-3">
              <FileCheck2 size={20} className="text-green-700 mt-0.5 shrink-0" />
              <div>
                <p className="text-sm font-bold text-gray-800">
                  Statement processed successfully
                </p>
                <p className="text-xs text-gray-500 mt-1">
                  {uploadedName}
                  {rowsWritten != null && <> — {rowsWritten.toLocaleString('en-IN')} rows written</>}
                  {' '}(task <span className="font-mono">{receipt.task_id}</span>). Choose the next step:
                </p>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <button
                onClick={() => setActiveTab('reconcile')}
                className="flex flex-col items-start gap-1 p-4 bg-white border border-gray-300 rounded-lg text-left hover:border-vv-burgundy hover:bg-vv-cream transition-colors"
              >
                <span className="flex items-center gap-2 text-sm font-bold text-gray-800">
                  <Scale size={16} className="text-vv-burgundy" />
                  Reconcile Statement
                </span>
                <span className="text-xs text-gray-500">
                  Run automated matching rules against internal ledgers.
                </span>
              </button>

              <button
                onClick={() => setActiveTab('analysis')}
                className="flex flex-col items-start gap-1 p-4 bg-white border border-gray-300 rounded-lg text-left hover:border-vv-burgundy hover:bg-vv-cream transition-colors"
              >
                <span className="flex items-center gap-2 text-sm font-bold text-gray-800">
                  <Activity size={16} className="text-vv-burgundy" />
                  Run Multi-Algorithm Risk Analysis
                </span>
                <span className="text-xs text-gray-500">
                  Scan for temporal anomalies, DBSCAN clusters, and risk scores.
                </span>
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
