import React from 'react';
import { Clock, CheckCircle, XCircle, Trash2, Calendar, PlayCircle } from 'lucide-react';
import { toast } from 'sonner';

export default function QueueList({ queue, history }) {
    // Safety checks
    const safeQueue = Array.isArray(queue) ? queue : [];
    const safeHistory = Array.isArray(history) ? history : [];

    const handleDelete = async (id) => {
        try {
            await fetch(`/api/queue/${id}`, { method: 'DELETE' });
            toast.success('Job removed from queue');
        } catch (err) {
            toast.error('Failed to remove job');
        }
    };

    return (
        <div className="space-y-6">
            {/* Pending Queue */}
            <div className="bg-slate-900 rounded-xl border border-slate-800 overflow-hidden">
                <div className="p-4 border-b border-slate-800 flex items-center gap-2">
                    <Clock className="w-5 h-5 text-blue-400" />
                    <h3 className="font-bold text-white">Pending Jobs ({safeQueue.length})</h3>
                </div>
                <div className="divide-y divide-slate-800">
                    {safeQueue.length === 0 ? (
                        <div className="p-6 text-center text-slate-500 text-sm">
                            No jobs in queue. Start a new job to add it here.
                        </div>
                    ) : (
                        safeQueue.map((job) => (
                            <div key={job.id} className="p-4 flex items-center justify-between hover:bg-slate-800/50 transition-colors">
                                <div>
                                    <div className="flex items-center gap-2 mb-1">
                                        <span className="text-white font-medium">
                                            {job.config?.inputDb?.split('/').pop() || job.name || 'Job'}
                                        </span>
                                        {job.scheduledFor && (
                                            <span className="text-xs bg-purple-500/20 text-purple-400 px-2 py-0.5 rounded-full flex items-center gap-1">
                                                <Calendar className="w-3 h-3" />
                                                {new Date(job.scheduledFor).toLocaleString()}
                                            </span>
                                        )}
                                    </div>
                                    <div className="text-xs text-slate-400 flex gap-3">
                                        <span>ID: {job.id}</span>
                                        {job.config?.outputDb && <span>Output: {job.config.outputDb?.split('/').pop()}</span>}
                                        {job.config?.concurrency && <span>Concurrency: {job.config.concurrency}</span>}
                                    </div>
                                </div>
                                <button
                                    onClick={() => handleDelete(job.id)}
                                    className="p-2 text-slate-500 hover:text-red-400 hover:bg-red-500/10 rounded-lg transition-colors"
                                    title="Remove from queue"
                                >
                                    <Trash2 className="w-4 h-4" />
                                </button>
                            </div>
                        ))
                    )}
                </div>
            </div>

            {/* History */}
            <div className="bg-slate-900 rounded-xl border border-slate-800 overflow-hidden">
                <div className="p-4 border-b border-slate-800 flex items-center gap-2">
                    <PlayCircle className="w-5 h-5 text-slate-400" />
                    <h3 className="font-bold text-white">Job History</h3>
                </div>
                <div className="divide-y divide-slate-800 max-h-[400px] overflow-y-auto">
                    {safeHistory.length === 0 ? (
                        <div className="p-6 text-center text-slate-500 text-sm">
                            No job history yet.
                        </div>
                    ) : (
                        safeHistory.map((job) => {
                            const stats = job.returnvalue || {};
                            const durationMs = job.finishedAt && job.processedOn ? job.finishedAt - job.processedOn : 0;
                            const durationSec = durationMs / 1000;
                            const durationFormatted = durationSec > 60
                                ? `${Math.floor(durationSec / 60)}m ${Math.round(durationSec % 60)}s`
                                : `${Math.round(durationSec)}s`;

                            const processed = stats.progress?.processed || 0;
                            const total = stats.progress?.total || 0;
                            const extracted = stats.progress?.extracted || 0;
                            const failed = stats.progress?.failed || 0;

                            const avgReqPerSec = durationSec > 0 ? (processed / durationSec).toFixed(2) : '0.00';
                            const successRate = processed > 0 ? ((extracted / processed) * 100).toFixed(1) : '0.0';

                            return (
                                <div key={job.id} className="p-4 hover:bg-slate-800/50 transition-colors">
                                    <div className="flex items-center justify-between mb-2">
                                        <div className="flex items-center gap-2">
                                            <span className="text-white font-medium text-sm">
                                                {job.config?.inputDb?.split('/').pop() || job.name || 'Job'}
                                            </span>
                                            <span className={`text-[10px] px-2 py-0.5 rounded-full flex items-center gap-1 uppercase tracking-wider font-bold ${job.status === 'completed'
                                                    ? 'bg-green-500/20 text-green-400 border border-green-500/20'
                                                    : 'bg-red-500/20 text-red-400 border border-red-500/20'
                                                }`}>
                                                {job.status === 'completed' ? <CheckCircle className="w-2.5 h-2.5" /> : <XCircle className="w-2.5 h-2.5" />}
                                                {job.status}
                                            </span>
                                        </div>
                                        <div className="text-[11px] text-slate-500 font-mono">
                                            {new Date(job.finishedAt).toLocaleTimeString()}
                                        </div>
                                    </div>

                                    <div className="grid grid-cols-4 gap-4 mt-3">
                                        <div className="flex flex-col">
                                            <span className="text-[10px] text-slate-500 uppercase font-bold">Duration</span>
                                            <span className="text-xs text-slate-300">{durationFormatted}</span>
                                        </div>
                                        <div className="flex flex-col">
                                            <span className="text-[10px] text-slate-500 uppercase font-bold">Progress</span>
                                            <span className="text-xs text-slate-300">{processed} / {total}</span>
                                        </div>
                                        <div className="flex flex-col">
                                            <span className="text-[10px] text-slate-500 uppercase font-bold">Avg Speed</span>
                                            <span className="text-xs text-blue-400 font-semibold">{avgReqPerSec} req/s</span>
                                        </div>
                                        <div className="flex flex-col">
                                            <span className="text-[10px] text-slate-500 uppercase font-bold">Success</span>
                                            <span className="text-xs text-green-400 font-semibold">{successRate}% OK</span>
                                        </div>
                                    </div>

                                    {failed > 0 && (
                                        <div className="mt-2 text-[10px] text-red-400/70">
                                            ⚠️ {failed} hotels failed to process
                                        </div>
                                    )}
                                </div>
                            );
                        })
                    )}
                </div>
            </div>
        </div>
    );
}
