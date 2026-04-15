/**
 * Advanced Metrics Tracker for Scraper Performance
 * Tracks real-time and rolling statistics for dashboard visualization
 */

export class MetricsTracker {
    constructor(scrapeRunId, db) {
        this.scrapeRunId = scrapeRunId;
        this.db = db;

        // Session tracking
        this.sessionStartTime = Date.now();
        this.lastSaveTime = Date.now();
        this.saveInterval = 30000; // Save to DB every 30 seconds

        // Request tracking
        this.requestTimestamps = [];
        this.latencies = [];
        this.errors = {
            total: 0,
            wafBlocked: 0,
            timeouts: 0,
            proxyErrors: 0
        };

        // Performance tracking
        this.totalRequests = 0;
        this.successfulRequests = 0;
        this.failedRequests = 0;
        this.peakRequestsPerSecond = 0;

        // Rolling windows
        this.WINDOW_10_MINUTES = 10 * 60 * 1000; // 10 minutes in ms
        this.WINDOW_1_MINUTE = 60 * 1000; // 1 minute in ms
        this.REQUEST_HISTORY_WINDOW = this.WINDOW_10_MINUTES;

        // Latest stats cache
        this.latestStats = {};

        // Auto-save interval
        this.autoSaveTimer = setInterval(() => this.saveMetricsToDb(), this.saveInterval);
    }

    /**
     * Track a new request
     */
    trackRequest(success = true, latencyMs = null) {
        const now = Date.now();
        this.requestTimestamps.push(now);
        this.totalRequests++;

        if (success) {
            this.successfulRequests++;
        } else {
            this.failedRequests++;
        }

        if (latencyMs !== null) {
            this.latencies.push(latencyMs);
            // Keep only last 1000 latencies to avoid memory issues
            if (this.latencies.length > 1000) {
                this.latencies.shift();
            }
        }

        // Cleanup old timestamps
        this.cleanupOldTimestamps();
    }

    /**
     * Track an error
     */
    trackError(errorType = 'general') {
        this.errors.total++;

        switch (errorType) {
            case 'waf':
            case 'waf_blocked':
                this.errors.wafBlocked++;
                break;
            case 'timeout':
                this.errors.timeouts++;
                break;
            case 'proxy':
            case 'proxy_error':
                this.errors.proxyErrors++;
                break;
        }
    }

    /**
     * Remove timestamps older than the tracking window
     */
    cleanupOldTimestamps() {
        const now = Date.now();
        const cutoff = now - this.REQUEST_HISTORY_WINDOW;
        this.requestTimestamps = this.requestTimestamps.filter(t => t > cutoff);
    }

    /**
     * Calculate current requests per second (last 1 second)
     */
    getCurrentRequestsPerSecond() {
        const now = Date.now();
        const oneSecondAgo = now - 1000;
        const recentRequests = this.requestTimestamps.filter(t => t > oneSecondAgo);
        return recentRequests.length;
    }

    /**
     * Calculate average requests per second over last 10 minutes
     */
    getAvgRequestsPerSecond10m() {
        const now = Date.now();
        const elapsedSeconds = Math.min((now - this.sessionStartTime) / 1000, 600); // Max 10 minutes

        if (elapsedSeconds === 0) return 0;

        const recentRequests = this.requestTimestamps.filter(
            t => t > now - this.WINDOW_10_MINUTES
        );

        return parseFloat((recentRequests.length / elapsedSeconds).toFixed(2));
    }

    /**
     * Calculate instantaneous req/s (last 10 seconds average)
     */
    getInstantRequestsPerSecond() {
        const now = Date.now();
        const tenSecondsAgo = now - 10000;
        const recentRequests = this.requestTimestamps.filter(t => t > tenSecondsAgo);
        return parseFloat((recentRequests.length / 10).toFixed(2));
    }

    /**
     * Update peak req/s if current is higher
     */
    updatePeakRequestsPerSecond() {
        const current = this.getInstantRequestsPerSecond();
        if (current > this.peakRequestsPerSecond) {
            this.peakRequestsPerSecond = current;
        }
        return this.peakRequestsPerSecond;
    }

    /**
     * Calculate latency statistics
     */
    getLatencyStats() {
        if (this.latencies.length === 0) {
            return { avg: 0, median: 0, p95: 0 };
        }

        const sorted = [...this.latencies].sort((a, b) => a - b);
        const avg = this.latencies.reduce((sum, l) => sum + l, 0) / this.latencies.length;
        const median = sorted[Math.floor(sorted.length / 2)];
        const p95Index = Math.floor(sorted.length * 0.95);
        const p95 = sorted[p95Index] || sorted[sorted.length - 1];

        return {
            avg: parseFloat(avg.toFixed(2)),
            median: parseFloat(median.toFixed(2)),
            p95: parseFloat(p95.toFixed(2))
        };
    }

    /**
     * Calculate error rate percentage
     */
    getErrorRate() {
        if (this.totalRequests === 0) return 0;
        return parseFloat(((this.failedRequests / this.totalRequests) * 100).toFixed(2));
    }

    /**
     * Get session uptime in seconds
     */
    getSessionUptime() {
        return Math.floor((Date.now() - this.sessionStartTime) / 1000);
    }

    /**
     * Get formatted uptime string
     */
    getFormattedUptime() {
        const seconds = this.getSessionUptime();
        const hours = Math.floor(seconds / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        const secs = seconds % 60;

        if (hours > 0) {
            return `${hours}h ${minutes}m ${secs}s`;
        } else if (minutes > 0) {
            return `${minutes}m ${secs}s`;
        } else {
            return `${secs}s`;
        }
    }

    /**
     * Update metrics from scraper stats (from __STATS__: JSON)
     */
    updateFromScraperStats(stats) {
        // Update error counts from stats
        if (stats.errors) {
            this.errors.total = stats.errors.total || 0;
            this.errors.wafBlocked = stats.errors.wafBlocked || 0;
            this.errors.timeouts = stats.errors.timeouts || 0;
        }

        // Cache the latest stats for DB save
        this.latestStats = stats;

        // Update peak
        this.updatePeakRequestsPerSecond();
    }

    /**
     * Get comprehensive metrics snapshot
     */
    getMetricsSnapshot() {
        const latencyStats = this.getLatencyStats();

        return {
            // Request metrics
            requestsPerSecond: this.getInstantRequestsPerSecond(),
            avgRequestsPerSecond10m: this.getAvgRequestsPerSecond10m(),
            peakRequestsPerSecond: this.peakRequestsPerSecond,
            totalRequests: this.totalRequests,
            successfulRequests: this.successfulRequests,
            failedRequests: this.failedRequests,

            // Performance metrics
            avgLatencyMs: latencyStats.avg,
            medianLatencyMs: latencyStats.median,
            p95LatencyMs: latencyStats.p95,

            // Error tracking
            errorRate: this.getErrorRate(),
            wafBlockedCount: this.errors.wafBlocked,
            timeoutCount: this.errors.timeouts,

            // Session info
            sessionUptime: this.getSessionUptime(),
            sessionUptimeFormatted: this.getFormattedUptime(),

            // Pool health (from scraper stats if available)
            poolSize: this.latestStats.pool?.poolSize || 0,
            poolHealthScore: this.latestStats.pool?.healthScore || 0,
            tokenConsumptionRate: this.latestStats.performance?.consumption || 0,

            // Proxy metrics (from scraper stats if available)
            proxySuccessRate: this.latestStats.pool?.refillSuccessRate || 0,
            activeProxies: this.latestStats.pool?.poolSize || 0,

            // Timestamp
            timestamp: new Date()
        };
    }

    /**
     * Save metrics to database
     */
    async saveMetricsToDb() {
        if (!this.db || !this.scrapeRunId) return;

        const now = Date.now();
        // Don't save too frequently
        if (now - this.lastSaveTime < this.saveInterval) return;

        try {
            const metrics = this.getMetricsSnapshot();

            await this.db.query(`
                INSERT INTO scraper_metrics_timeseries (
                    scrape_run_id,
                    timestamp,
                    requests_per_second,
                    avg_requests_per_second_10m,
                    peak_requests_per_second,
                    total_requests,
                    successful_requests,
                    failed_requests,
                    avg_latency_ms,
                    median_latency_ms,
                    p95_latency_ms,
                    error_rate,
                    waf_blocked_count,
                    timeout_count,
                    pool_size,
                    pool_health_score,
                    token_consumption_rate,
                    proxy_success_rate,
                    active_proxies
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
            `, [
                this.scrapeRunId,
                metrics.timestamp,
                metrics.requestsPerSecond,
                metrics.avgRequestsPerSecond10m,
                metrics.peakRequestsPerSecond,
                metrics.totalRequests,
                metrics.successfulRequests,
                metrics.failedRequests,
                metrics.avgLatencyMs,
                metrics.medianLatencyMs,
                metrics.p95LatencyMs,
                metrics.errorRate,
                metrics.wafBlockedCount,
                metrics.timeoutCount,
                metrics.poolSize,
                metrics.poolHealthScore,
                metrics.tokenConsumptionRate,
                metrics.proxySuccessRate,
                metrics.activeProxies
            ]);

            this.lastSaveTime = now;
            console.log(`📊 Metrics saved to DB for run ${this.scrapeRunId}`);
        } catch (err) {
            console.error('❌ Error saving metrics to DB:', err);
        }
    }

    /**
     * Force save metrics and cleanup
     */
    async finalize() {
        clearInterval(this.autoSaveTimer);
        await this.saveMetricsToDb();
    }

    /**
     * Cleanup resources
     */
    destroy() {
        clearInterval(this.autoSaveTimer);
        this.requestTimestamps = [];
        this.latencies = [];
    }
}

export default MetricsTracker;
