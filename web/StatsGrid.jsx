import React from 'react';
import { Activity, AlertTriangle, CheckCircle2, Clock3, Cpu, MemoryStick, Shield, Timer } from 'lucide-react';

function formatDuration(seconds) {
    if (!seconds || seconds < 0) return '0s';
    const total = Math.floor(seconds);
    const hours = Math.floor(total / 3600);
    const minutes = Math.floor((total % 3600) / 60);
    const secs = total % 60;
    if (hours > 0) return `${hours}h ${minutes}m`;
    if (minutes > 0) return `${minutes}m ${secs}s`;
    return `${secs}s`;
}

function formatEtaTimestamp(seconds) {
    if (!Number.isFinite(seconds) || seconds < 0) return 'finish time n/a';
    try {
        return new Date(Date.now() + (seconds * 1000)).toLocaleTimeString('it-IT', {
            hour: '2-digit',
            minute: '2-digit'
        });
    } catch {
        return 'finish time n/a';
    }
}

function formatBytes(bytes) {
    if (!bytes) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let value = bytes;
    let index = 0;
    while (value >= 1024 && index < units.length - 1) {
        value /= 1024;
        index++;
    }
    return `${value.toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

export default function StatsGrid({ stats, systemStats }) {
    if (!stats) {
        return (
            <div className="rounded-2xl border border-slate-800 bg-slate-900/40 p-8 text-center text-slate-500">
                Waiting for runtime stats.
            </div>
        );
    }

    const progress = stats.progress || {};
    const performance = stats.performance || {};
    const errors = stats.errors || {};
    const timing = stats.timing || {};
    const activity = stats.activity || {};

    const progressPercent = progress.completionPct || (progress.total > 0 ? (progress.processed / progress.total) * 100 : 0);
    const successfulRequests = Number.isFinite(progress.successfulRequests)
        ? Number(progress.successfulRequests)
        : Math.max(0, (progress.processed || 0) - (progress.failed || errors.total || 0));
    const requestSuccessRate = (progress.processed || 0) > 0
        ? (successfulRequests / progress.processed) * 100
        : 0;
    const latencyLabel = Number.isFinite(performance.latency) && performance.latency > 0
        ? `avg latency ${performance.latency.toFixed(0)} ms`
        : 'avg latency n/a';
    const etaLabel = Number.isFinite(timing.eta) ? formatDuration(timing.eta) : 'n/a';
    const etaClock = Number.isFinite(timing.eta) ? formatEtaTimestamp(timing.eta) : 'finish time n/a';

    return (
        <div className="space-y-4">
            {systemStats && (
                <div className="grid gap-4 md:grid-cols-3">
                    <StatCard
                        title="Server CPU"
                        value={`${(systemStats.cpu || 0).toFixed(1)}%`}
                        subtle={`${navigator.hardwareConcurrency || 'N/A'} cores`}
                        icon={Cpu}
                    />
                    <StatCard
                        title="Server Memory"
                        value={formatBytes(systemStats.memory?.used || 0)}
                        subtle={`${(systemStats.memory?.usagePercent || 0).toFixed(1)}% of ${formatBytes(systemStats.memory?.total || 0)}`}
                        icon={MemoryStick}
                    />
                    <StatCard
                        title="Uptime"
                        value={formatDuration(systemStats.uptime || 0)}
                        subtle="host runtime"
                        icon={Timer}
                    />
                </div>
            )}

            <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-5">
                <div className="flex items-center justify-between gap-4">
                    <div>
                        <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Progress</div>
                        <div className="mt-1 text-2xl font-semibold text-white">{progressPercent.toFixed(1)}%</div>
                    </div>
                    <div className="text-right text-sm text-slate-400">
                        <div>{progress.processed || 0}/{progress.total || 0} processed</div>
                        <div>{progress.remaining || 0} remaining</div>
                    </div>
                </div>
                <div className="mt-4 h-3 overflow-hidden rounded-full bg-slate-800">
                    <div
                        className="h-full rounded-full bg-gradient-to-r from-blue-500 to-cyan-400 transition-all"
                        style={{ width: `${Math.max(0, Math.min(100, progressPercent))}%` }}
                    />
                </div>
            </div>

            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                <StatCard
                    title="Successful Req"
                    value={successfulRequests}
                    subtle={`${requestSuccessRate.toFixed(1)}% request success`}
                    icon={CheckCircle2}
                />
                <StatCard
                    title="Errors"
                    value={errors.total || 0}
                    subtle={`${(errors.rate || 0).toFixed(1)}% failed`}
                    icon={AlertTriangle}
                />
                <StatCard
                    title="WAF / 429"
                    value={errors.wafBlocked || 0}
                    subtle={`${errors.timeouts || 0} timeouts`}
                    icon={Shield}
                />
                <StatCard
                    title="Speed"
                    value={`${(performance.speed10s || performance.speed || 0).toFixed(1)} req/s`}
                    subtle={latencyLabel}
                    icon={Activity}
                />
            </div>

            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                <StatCard
                    title="Elapsed"
                    value={timing.elapsedLabel || formatDuration(timing.elapsed || 0)}
                    subtle={timing.eta ? `ETA ${etaLabel}` : 'ETA n/a'}
                    icon={Clock3}
                />
                <StatCard
                    title="ETA"
                    value={etaLabel}
                    subtle={etaClock}
                    icon={Timer}
                />
                <StatCard
                    title="Last Hotel"
                    value={activity.lastHotel || 'n/a'}
                    subtle="latest completed request"
                    icon={Activity}
                />
                <StatCard
                    title="Last Outcome"
                    value={activity.lastStatus || 'n/a'}
                    subtle={`${stats.activeShards || 0} active shards`}
                    icon={AlertTriangle}
                />
            </div>
        </div>
    );
}

function StatCard({ title, value, subtle, icon: Icon }) {
    return (
        <div className="rounded-2xl border border-slate-800 bg-slate-900/60 p-4">
            <div className="flex items-center justify-between gap-3">
                <div className="text-xs uppercase tracking-[0.18em] text-slate-500">{title}</div>
                <Icon className="h-4 w-4 text-slate-500" />
            </div>
            <div className="mt-2 text-lg font-semibold text-white break-words">{value}</div>
            <div className="mt-1 text-xs text-slate-500">{subtle}</div>
        </div>
    );
}
