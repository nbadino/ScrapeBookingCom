import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';
import os from 'os';
import db, { getClient } from '../db/index.js';
import { MetricsTracker } from './metricsTracker.js';
import { scrapeQueue, worker, activeRuns, cancelJob } from '../queue/index.js';
import { DATA_DIR, PROXY_CHAIN_CONFIG_FILE, PROXY_FILE as SHARED_PROXY_FILE, DEBUG_HTML_DIR, GO_SCRAPER_BINARY } from '../config.js';
import { applyScraperDefaults } from './scraperConfigStore.js';
import { telegramService } from './telegramService.js';
import { buildMonitorHotelFilterQuery } from './monitorFilters.js';
import { normalizeScraperStats } from './normalizeScraperStats.js';

// New Modules
import { LogForwarder } from './LogForwarder.js';
import { StatsAggregator } from './StatsAggregator.js';
import { ProcessCoordinator } from './ProcessCoordinator.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROXY_FILE = SHARED_PROXY_FILE;
const PRICE_JOB_NAMES = new Set(['scrape-prices', 'monitor-execute']);
const DISCOVERY_JOB_NAMES = new Set([
    'scrape-discovery',
    'scrape-hotels-from-sitemap',
    'scrape-hotel-details-from-sitemap',
    'download-sitemap'
]);

class ScraperManager {
    constructor() {
        this.io = null;

        // Modules
        this.logForwarder = null;
        this.statsAggregator = null;
        this.processCoordinator = null;

        // State (maintained for internal logic and initial socket emission)
        this.isRunning = false;
        this.startTime = null;
        this.currentConfig = null;
        this.metricsTracker = null;
        this.currentScrapeRunId = null;
        this.currentQueueJobId = null;

        this.isPriceScraperRunning = false;
        this.priceScraperStartTime = null;
        this.priceScraperConfig = null;
        this.priceScraperMetricsTracker = null;
        this.priceScraperScrapeRunId = null;
        this.priceScraperStats = null;

        this.monitorRunState = {
            monitorRunId: null,
            totalDates: 0,
            currentDateIndex: 0,
            checkinDates: [],
            aggregatedStats: null
        };

        this.activePriceScraperJobIds = new Set();
    }

    buildPlaceholderStats({ activeShards = 0, lastStatus = 'starting' } = {}) {
        return {
            sourceKind: 'placeholder',
            activeShards,
            progress: {
                total: 0,
                processed: 0,
                extracted: 0,
                failed: 0,
                remaining: 0,
                completionPct: 0,
                successRate: 0
            },
            performance: {
                speed: 0,
                speed10s: 0,
                speed60s: 0,
                latency: 0
            },
            errors: {
                total: 0,
                wafBlocked: 0,
                timeouts: 0,
                networkErrors: 0,
                rate: 0
            },
            timing: {
                elapsed: 0,
                eta: null
            },
            activity: {
                lastHotel: null,
                lastStatus
            }
        };
    }

    init(io) {
        this.io = io;
        this.logForwarder = new LogForwarder(io);
        this.statsAggregator = new StatsAggregator(io);
        this.processCoordinator = new ProcessCoordinator(io);

        this.setupSocketEvents();
        this.setupWorkerEvents();
        // Flush loops
        setInterval(() => this.logForwarder.flush(), 1000);
        setInterval(() => {
            if (this.io) this.io.emit('systemStats', this.getSystemStats());
        }, 5000);
    }

    loadProxyStrategy() {
        const defaults = {
            mode: 'list',
            enabled: false,
            socksHost: 'localhost',
            socksPort: 1080,
            rotatingProxy: ''
        };
        try {
            if (fs.existsSync(PROXY_CHAIN_CONFIG_FILE)) {
                const content = fs.readFileSync(PROXY_CHAIN_CONFIG_FILE, 'utf-8');
                return { ...defaults, ...JSON.parse(content) };
            }
        } catch (err) {
            console.error('⚠️ Failed to read proxy strategy config:', err.message);
        }
        return defaults;
    }

    handlePriceScraperStats(statsJson) {
        const normalizedStats = normalizeScraperStats(statsJson);
        const currentProcessStats = this.statsAggregator.aggregateStats([normalizedStats]);
        const currentDateIndex = this.monitorRunState.currentDateIndex;

        if (!this.monitorRunState.perDateStats) this.monitorRunState.perDateStats = {};

        this.monitorRunState.perDateStats[currentDateIndex] = {
            processed: currentProcessStats.progress?.processed || 0,
            extracted: currentProcessStats.progress?.extracted || 0,
            failed: currentProcessStats.progress?.failed || 0,
            total: currentProcessStats.progress?.total || 0
        };

        let totalProcessed = 0, totalExtracted = 0, totalFailed = 0, overallTotal = 0;
        for (const ds of Object.values(this.monitorRunState.perDateStats)) {
            totalProcessed += ds.processed;
            totalExtracted += ds.extracted;
            totalFailed += ds.failed;
            overallTotal += ds.total;
        }

        const aggregatedStats = {
            ...currentProcessStats,
            progress: {
                processed: totalProcessed,
                extracted: totalExtracted,
                failed: totalFailed,
                total: this.monitorRunState.totalExpected || overallTotal, // Use calculated global total if available
                remaining: Math.max(0, (this.monitorRunState.totalExpected || overallTotal) - totalProcessed)
            },
            monitorProgress: {
                currentDateIndex: currentDateIndex,
                totalDates: this.monitorRunState.totalDates,
                currentDate: this.monitorRunState.checkinDates[currentDateIndex]
            }
        };

        this.priceScraperStats = aggregatedStats;

        if (this.priceScraperMetricsTracker) {
            this.priceScraperMetricsTracker.updateFromScraperStats(normalizedStats);
            const advancedMetrics = this.priceScraperMetricsTracker.getMetricsSnapshot();
            aggregatedStats.advanced = { ...advancedMetrics, scrapeRunId: this.priceScraperScrapeRunId };
        }

        if (this.io) this.io.emit('priceScraperStats', aggregatedStats);
    }

    getSystemStats() {
        const totalMem = os.totalmem();
        let freeMem = os.freemem();
        if (process.platform === 'linux') {
            try {
                const memInfo = fs.readFileSync('/proc/meminfo', 'utf8');
                const match = memInfo.match(/MemAvailable:\s+(\d+)\s+kB/);
                if (match && match[1]) freeMem = parseInt(match[1], 10) * 1024;
            } catch (e) { }
        }
        const usedMem = totalMem - freeMem;
        return {
            cpu: os.loadavg()[0],
            memory: { total: totalMem, free: freeMem, used: usedMem, usagePercent: Math.round((usedMem / totalMem) * 100) },
            uptime: os.uptime()
        };
    }

    async refreshRuntimeStateFromQueue() {
        const jobs = await scrapeQueue.getJobs(['active', 'waiting', 'delayed']);
        const hasPriceJob = jobs.some((job) => PRICE_JOB_NAMES.has(job.name));
        const hasDiscoveryJob = jobs.some((job) => DISCOVERY_JOB_NAMES.has(job.name));

        this.isPriceScraperRunning = hasPriceJob || !!this.processCoordinator?.priceScraperProcess;
        this.isRunning = hasDiscoveryJob || !!this.processCoordinator?.scraperProcess;

        if (this.isPriceScraperRunning && !this.priceScraperStats) {
            this.priceScraperStats = this.statsAggregator.lastStats
                || this.buildPlaceholderStats({
                    activeShards: jobs.filter((job) => PRICE_JOB_NAMES.has(job.name)).length,
                    lastStatus: 'queued'
                });
        }

        return {
            hasPriceJob,
            hasDiscoveryJob,
            jobs
        };
    }

    setupWorkerEvents() {
        if (!worker) return;
        const hotelSitemapJobs = new Set(['scrape-hotels-from-sitemap', 'scrape-hotel-details-from-sitemap']);

        worker.on('jobLog', (data) => this.logForwarder.broadcastJobLog(data, this.activePriceScraperJobIds));
        worker.on('jobStats', (data) => {
            this.statsAggregator.handleJobStats(data);

            if (this.io) {
                this.io.emit('jobStats', data);
            }

            const isPriceJob = PRICE_JOB_NAMES.has(data?.jobName) || this.activePriceScraperJobIds.has(String(data?.jobId));
            if (isPriceJob) {
                const targetJobIds = new Set(this.activePriceScraperJobIds);
                if (data?.jobId) targetJobIds.add(String(data.jobId));
                const aggregatedPriceStats = this.statsAggregator.getAggregatedStatsForJobs(targetJobIds);
                if (aggregatedPriceStats) {
                    this.priceScraperStats = aggregatedPriceStats;
                    if (this.io) this.io.emit('priceScraperStats', aggregatedPriceStats);
                }
            }
        });

        worker.on('completed', (job) => {
            this.statsAggregator.clearJob(job.id);

            if (hotelSitemapJobs.has(job.name) && this.isRunning) {
                this.isRunning = false;
                this.startTime = null;
                this.currentQueueJobId = null;
                if (this.io) this.io.emit('status', { isRunning: false, startTime: null });
            }

            if (PRICE_JOB_NAMES.has(job.name)) {
                this.activePriceScraperJobIds.delete(job.id);
                if (this.activePriceScraperJobIds.size === 0 && this.isPriceScraperRunning) {
                    this.isPriceScraperRunning = false;
                    this.priceScraperStartTime = null;
                    if (this.io) this.io.emit('priceScraperStatus', { isRunning: false, startTime: null });
                }
            }

            if (this.io) {
                this.io.emit('jobCompleted', {
                    job: {
                        id: job.id,
                        name: job.name,
                        data: job.data,
                        progress: job.progress || 100,
                        processedOn: job.processedOn,
                        finishedOn: job.finishedOn,
                        returnvalue: job.returnvalue
                    },
                    status: 'completed'
                });
                this.emitQueueUpdate();
            }
            telegramService.sendJobCompleted(job.name, job.id, job.returnvalue).catch(() => { });
        });

        worker.on('failed', (job, err) => {
            this.statsAggregator.clearJob(job.id);

            if (hotelSitemapJobs.has(job.name) && this.isRunning) {
                this.isRunning = false;
                this.startTime = null;
                this.currentQueueJobId = null;
                if (this.io) this.io.emit('status', { isRunning: false, startTime: null });
            }

            if (PRICE_JOB_NAMES.has(job.name)) {
                this.activePriceScraperJobIds.delete(job.id);
                if (this.activePriceScraperJobIds.size === 0 && this.isPriceScraperRunning) {
                    this.isPriceScraperRunning = false;
                    this.priceScraperStartTime = null;
                    if (this.io) this.io.emit('priceScraperStatus', { isRunning: false, startTime: null });
                }
            }

            if (this.io) {
                this.io.emit('jobFailed', { jobId: job.id, error: err.message });
                this.emitQueueUpdate();
            }
            telegramService.sendJobFailed(job.name, job.id, err.message).catch(() => { });
        });

        worker.on('active', (job) => {
            if (hotelSitemapJobs.has(job.name) && !this.isRunning) {
                this.isRunning = true;
                this.startTime = Date.now();
                this.currentQueueJobId = job.id;
                this.currentConfig = applyScraperDefaults(job.data || {}, 'hotel');
                if (this.io) {
                    this.io.emit('status', { isRunning: true, startTime: this.startTime, jobId: job.id });
                    this.io.emit('config', this.currentConfig);
                }
            }

            if (PRICE_JOB_NAMES.has(job.name)) {
                this.activePriceScraperJobIds.add(job.id);
                if (!this.isPriceScraperRunning) {
                    this.isPriceScraperRunning = true;
                    this.priceScraperStartTime = Date.now();
                    this.priceScraperConfig = applyScraperDefaults(job.data?.priceConfig || {}, 'price');
                    if (this.io) {
                        this.io.emit('priceScraperStatus', { isRunning: true, startTime: this.priceScraperStartTime });
                        this.io.emit('priceScraperConfig', this.priceScraperConfig);
                    }
                }

                this.priceScraperStats = this.buildPlaceholderStats({
                    activeShards: this.activePriceScraperJobIds.size,
                    lastStatus: 'starting'
                });
                if (this.io) {
                    this.io.emit('priceScraperStats', this.priceScraperStats);
                }
            }
            if (this.io) {
                this.io.emit('jobActive', { jobId: job.id });
                this.emitQueueUpdate();
            }
            telegramService.sendJobStarted(job.name, job.id, job.data).catch(() => { });
        });
    }

    setupSocketEvents() {
        if (!this.io) return;
        this.io.on('connection', async (socket) => {
            try {
                await this.refreshRuntimeStateFromQueue();
            } catch (err) {
                console.warn('Failed to refresh runtime state for socket:', err.message);
            }

            socket.emit('status', { isRunning: this.isRunning, startTime: this.startTime });
            socket.emit('logs', this.logForwarder.getDiscoveryLogs());
            if (this.statsAggregator.lastStats) socket.emit('stats', this.statsAggregator.lastStats);
            socket.emit('systemStats', this.getSystemStats());
            if (this.currentConfig) socket.emit('config', this.currentConfig);
            this.emitQueueUpdate();

            const priceStats = this.priceScraperStats || this.statsAggregator.lastStats;
            socket.emit('priceScraperStatus', { isRunning: this.isPriceScraperRunning, startTime: this.priceScraperStartTime });
            socket.emit('priceScraperLogs', this.logForwarder.getPriceScraperLogs());
            if (priceStats && this.isPriceScraperRunning) socket.emit('priceScraperStats', priceStats);
            if (this.priceScraperConfig) socket.emit('priceScraperConfig', this.priceScraperConfig);
        });
    }

    async emitQueueUpdate() {
        if (!this.io) return;
        try {
            await this.refreshRuntimeStateFromQueue();
        } catch (err) {
            console.warn('Failed to refresh runtime state before queue update:', err.message);
        }
        const data = await this.getQueueData();
        this.io.emit('queueUpdate', data);
    }

    async startScraper(config, jobId = null) {
        if (this.isRunning) throw new Error('Scraper is already running');
        const finalConfig = applyScraperDefaults(config, 'hotel');
        this.currentConfig = finalConfig;

        try {
            const result = await db.query(`
                INSERT INTO scrape_runs (started_at, target_site, status, notes)
                VALUES (NOW(), $1, 'running', $2)
                RETURNING scrape_run_id
            `, ['booking.com', `Manual Job ${jobId || 'manual'}`]);
            this.currentScrapeRunId = result.rows[0].scrape_run_id;
            this.metricsTracker = new MetricsTracker(this.currentScrapeRunId, db);
        } catch (err) { }

        const args = [
            '--concurrency', (finalConfig.concurrency || '10').toString(),
            '--db-url', process.env.DATABASE_URL || `postgres://${process.env.DB_USER}:${process.env.DB_PASS}@${process.env.DB_HOST}:${process.env.DB_PORT}/${process.env.DB_NAME}`,
            `--headless=${finalConfig.headless === false ? 'false' : 'true'}`
        ];

        if (finalConfig.useProxy) {
            if (finalConfig.proxyFile) args.push('--proxies', path.resolve(DATA_DIR, finalConfig.proxyFile));
            else if (fs.existsSync(SHARED_PROXY_FILE)) args.push('--proxies', SHARED_PROXY_FILE);
        }

        if (finalConfig.limit) {
            args.push('--limit', finalConfig.limit.toString());
        }

        // Check if we are doing discovery or specific search
        if (finalConfig.country) {
            args.push('--discover', '--country', finalConfig.country);
        }

        const proxyStrategy = this.loadProxyStrategy();
        const spawnEnv = { ...process.env, FORCE_COLOR: '1' };
        if (this.currentScrapeRunId) {
            spawnEnv.SCRAPE_RUN_ID = String(this.currentScrapeRunId);
        }
        if (proxyStrategy.mode === 'chain' && proxyStrategy.enabled) {
            spawnEnv.USE_PROXY_CHAIN = 'true';
            spawnEnv.SOCKS5_PROXY = `${proxyStrategy.socksHost}:${proxyStrategy.socksPort}`;
        } else if (proxyStrategy.mode === 'rotating' && proxyStrategy.rotatingProxy) {
            const isSocks = proxyStrategy.rotatingProxy.toLowerCase().startsWith('socks');
            spawnEnv.USE_ROTATING_SOCKS = isSocks ? 'true' : 'false';
            spawnEnv.ROTATING_SOCKS_PROXY = isSocks ? proxyStrategy.rotatingProxy : '';
            spawnEnv.ROTATING_HTTP_PROXY = isSocks ? '' : proxyStrategy.rotatingProxy;
        }

        this.processCoordinator.scraperProcess = this.processCoordinator.spawn(GO_SCRAPER_BINARY, args, spawnEnv, path.dirname(GO_SCRAPER_BINARY));
        const proc = this.processCoordinator.scraperProcess;

        this.isRunning = true;
        this.startTime = Date.now();

        if (this.io) this.io.emit('status', { isRunning: true, startTime: this.startTime, jobId });

        proc.stdout.on('data', (data) => {
            const str = data.toString();
            if (str.includes('__STATS__:')) {
                const parts = str.split('__STATS__:');
                for (let j = 1; j < parts.length; j++) {
                    try {
                        const jsonMatch = parts[j].match(/\{[\s\S]*?\}/);
                        const statsJson = JSON.parse(jsonMatch ? jsonMatch[0] : parts[j]);
                        const normalizedStats = normalizeScraperStats(statsJson);
                        if (this.metricsTracker) {
                            this.metricsTracker.updateFromScraperStats(normalizedStats);
                            const enhanced = { ...normalizedStats, advanced: { ...this.metricsTracker.getMetricsSnapshot(), scrapeRunId: this.currentScrapeRunId } };
                            if (this.io) this.io.emit('stats', enhanced);
                        } else {
                            if (this.io) this.io.emit('stats', normalizedStats);
                        }
                    } catch (err) { }
                }
                const log = parts[0].trim();
                if (log) this.logForwarder.pushLog(log);
            } else {
                this.logForwarder.pushLog(str);
            }
        });

        proc.stderr.on('data', (data) => this.logForwarder.pushLog(data.toString()));

        proc.on('close', async (code) => {
            this.isRunning = false;
            this.processCoordinator.scraperProcess = null;
            if (this.metricsTracker) {
                await this.metricsTracker.finalize();
                await db.query('UPDATE scrape_runs SET finished_at = NOW(), status = $1 WHERE scrape_run_id = $2', [code === 0 ? 'success' : 'failed', this.currentScrapeRunId]);
                this.metricsTracker = null;
            }
            if (this.io) this.io.emit('status', { isRunning: false, exitCode: code, startTime: null });
        });
    }

    async startPriceScraper(config) {
        if (this.isPriceScraperRunning) throw new Error('Price scraper is already running');
        const finalConfig = applyScraperDefaults(config, 'price');
        const checkinDates = [];
        const cursor = new Date(finalConfig.startDate);
        const nights = parseInt(finalConfig.nights || '1', 10);
        while (cursor <= new Date(finalConfig.endDate)) {
            checkinDates.push(cursor.toISOString().split('T')[0]);
            cursor.setDate(cursor.getDate() + nights);
        }

        this.priceScraperConfig = finalConfig;
        this.isPriceScraperRunning = true;
        this.priceScraperStartTime = Date.now();
        this.monitorRunState = { monitorRunId: null, totalDates: checkinDates.length, currentDateIndex: 0, checkinDates };
        this.activePriceScraperJobIds.clear();

        let monitorRunId = null;
        try {
            // Create Monitor Run
            const monitorRun = await db.query('INSERT INTO monitor_runs (status, started_at, notes) VALUES (\'running\', NOW(), $1) RETURNING monitor_run_id', [`Manual run ${checkinDates[0]} range`]);
            monitorRunId = monitorRun.rows[0].monitor_run_id;
            this.monitorRunState.monitorRunId = monitorRunId;

            // Calculate Total Expected Work (Approximate)
            let totalHotelsInMonitor = 0;
            let filterSource = {
                countries: config.filterCountries ?? config.filter_countries ?? null,
                regions: config.filterRegions ?? config.filter_regions ?? null,
                cities: config.filterCities ?? config.filter_cities ?? null,
                minStars: config.filterMinStars ?? config.filter_min_stars ?? null,
                maxStars: config.filterMaxStars ?? config.filter_max_stars ?? null,
                minSquareMeters: config.filterMinSquareMeters ?? config.filter_min_square_meters ?? null,
                maxSquareMeters: config.filterMaxSquareMeters ?? config.filter_max_square_meters ?? null,
                propertyTypes: config.filterPropertyTypes ?? config.filter_property_types ?? null
            };

            if (config.monitorId) {
                const monitorRes = await db.query(`
                    SELECT filter_countries, filter_regions, filter_cities,
                           filter_min_stars, filter_max_stars,
                           filter_min_square_meters, filter_max_square_meters,
                           filter_property_types
                    FROM monitoring_pipelines
                    WHERE monitor_id = $1
                `, [config.monitorId]);

                if (monitorRes.rows.length > 0) {
                    const monitor = monitorRes.rows[0];
                    filterSource = {
                        countries: monitor.filter_countries,
                        regions: monitor.filter_regions,
                        cities: monitor.filter_cities,
                        minStars: monitor.filter_min_stars,
                        maxStars: monitor.filter_max_stars,
                        minSquareMeters: monitor.filter_min_square_meters,
                        maxSquareMeters: monitor.filter_max_square_meters,
                        propertyTypes: monitor.filter_property_types
                    };
                }
            }

            const { whereClause, params } = buildMonitorHotelFilterQuery(filterSource);
            const countRes = await db.query(`
                SELECT COUNT(DISTINCT h.hotel_sk) as cnt
                FROM hotels h
                JOIN hotel_versions hv ON h.hotel_sk = hv.hotel_sk AND hv.valid_to IS NULL
                WHERE ${whereClause}
            `, params);
            totalHotelsInMonitor = parseInt(countRes.rows[0].cnt, 10);

            await db.query(`
                INSERT INTO monitor_run_history (
                    monitor_run_id, monitor_id, started_at, status, hotels_found, config_snapshot
                )
                VALUES ($1, $2, NOW(), 'running', $3, $4::jsonb)
            `, [
                monitorRunId,
                config.monitorId || null,
                totalHotelsInMonitor,
                JSON.stringify({
                    config: finalConfig,
                    checkinDates,
                    nights
                })
            ]);

            const totalExpected = totalHotelsInMonitor * checkinDates.length;
            this.monitorRunState.totalExpected = totalExpected; // Store for aggregation logic

            const jobsToQueue = [];

            // Prepare all search records first
            for (let i = 0; i < checkinDates.length; i++) {
                const { searchId, monitorRunDateId } = await this.preparePriceSearch(monitorRunId, checkinDates[i], nights, finalConfig);

                jobsToQueue.push({
                    name: 'scrape-prices',
                    data: {
                        priceConfig: finalConfig,
                        searchId,
                        monitorRunDateId,
                        checkinDate: checkinDates[i],
                        monitorId: config.monitorId,
                        monitorName: config.monitorName || 'Manual Run'
                    },
                    opts: {
                        jobId: `price-${searchId}-${Date.now()}`
                    }
                });
            }

            // Bulk add to queue
            await scrapeQueue.addBulk(jobsToQueue);

            console.log(`[ScraperManager] Enqueued ${jobsToQueue.length} jobs for monitor run ${monitorRunId}`);

            if (this.io) {
                this.io.emit('priceScraperStatus', { isRunning: true, startTime: this.priceScraperStartTime });
                this.io.emit('priceScraperConfig', this.priceScraperConfig);
                this.emitQueueUpdate();
            }

        } catch (err) {
            console.error('Failed to start price scraper:', err);
            if (monitorRunId) {
                await db.query('UPDATE monitor_runs SET status = \'failed\', finished_at = NOW() WHERE monitor_run_id = $1', [monitorRunId]).catch(() => { });
                await db.query(`
                    UPDATE monitor_run_history
                    SET status = 'failed',
                        completed_at = NOW(),
                        execution_time_seconds = EXTRACT(EPOCH FROM NOW() - started_at)::INT
                    WHERE monitor_run_id = $1
                `, [monitorRunId]).catch(() => { });
            }
            this.isPriceScraperRunning = false;
            this.priceScraperStartTime = null;
            if (this.io) this.io.emit('priceScraperStatus', { isRunning: false, startTime: null });
            throw err;
        }
    }

    async preparePriceSearch(monitorRunId, checkinDate, nights, config) {
        const adults = Number.isFinite(Number(config.adults)) && Number(config.adults) > 0
            ? Math.floor(Number(config.adults))
            : 2;
        const children = Number.isFinite(Number(config.children)) && Number(config.children) >= 0
            ? Math.floor(Number(config.children))
            : 0;
        const currency = String(config.currency || config.searchCurrency || 'EUR').trim().toUpperCase() || 'EUR';
        const locale = String(config.locale || 'it-IT').trim() || 'it-IT';
        const deviceType = String(config.deviceType || 'desktop').trim() || 'desktop';
        const marketCountry = String(config.marketCountry || config.country || '').trim().toUpperCase().slice(0, 2) || null;
        const rooms = Number.isFinite(Number(config.rooms)) && Number(config.rooms) > 0
            ? Math.floor(Number(config.rooms))
            : 1;
        const childrenAges = Array.isArray(config.childrenAges) ? JSON.stringify(config.childrenAges) : null;

        const client = await getClient();
        try {
            await client.query('BEGIN');

            const mrdRes = await client.query(`
                INSERT INTO monitor_run_dates (
                    monitor_run_id, checkin_date, nights, adults, children, children_ages, currency
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING monitor_run_date_id
            `,
            [
                monitorRunId,
                checkinDate,
                nights,
                adults,
                children,
                childrenAges,
                currency
            ]);
            const mrdId = mrdRes.rows[0].monitor_run_date_id;

            const sRes = await client.query(`
                INSERT INTO searches (
                    search_timestamp, checkin_date, length_of_stay,
                    occupancy_adults, occupancy_children, rooms, children_ages,
                    currency, locale, device_type, market_country,
                    monitor_run_date_id
                ) VALUES (NOW(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                RETURNING search_id
            `,
            [
                checkinDate,
                nights,
                adults,
                children,
                rooms,
                childrenAges,
                currency,
                locale,
                deviceType,
                marketCountry,
                mrdId
            ]);

            await client.query('COMMIT');
            return { searchId: sRes.rows[0].search_id, monitorRunDateId: mrdId };
        } catch (err) {
            await client.query('ROLLBACK').catch(() => { });
            throw err;
        } finally {
            client.release();
        }
    }

    // Deprecated: used for old sequential spawn
    async runPriceScraperProcess(searchId, monitorRunDateId, config, checkinDate) {
        // No-op or keep for reference if needed
        return Promise.resolve();
    }

    async runPriceScraperProcess(searchId, monitorRunDateId, config, checkinDate) {
        const args = [
            '--search-id', searchId.toString(),
            '--db-url', process.env.DATABASE_URL || `postgres://${process.env.DB_USER}:${process.env.DB_PASS}@${process.env.DB_HOST}:${process.env.DB_PORT}/${process.env.DB_NAME}`,
            '--concurrency', (config.concurrency || 10).toString(),
            `--headless=${config.headless === false ? 'false' : 'true'}`
        ];

        if (config.useProxy && fs.existsSync(SHARED_PROXY_FILE)) {
            args.push('--proxies', SHARED_PROXY_FILE);
        }

        const spawnEnv = { ...process.env };
        if (this.currentScrapeRunId) {
            spawnEnv.SCRAPE_RUN_ID = String(this.currentScrapeRunId);
        }

        const proc = this.processCoordinator.spawn(GO_SCRAPER_BINARY, args, spawnEnv, path.dirname(GO_SCRAPER_BINARY));
        this.processCoordinator.priceScraperProcess = proc;

        return new Promise((resolve, reject) => {
            proc.stdout.on('data', (d) => {
                const s = d.toString();
                if (s.includes('__STATS__:')) this.handlePriceScraperStats(JSON.parse(s.split('__STATS__:')[1].trim()));
                else this.logForwarder.logPriceScraper(s);
            });
            proc.on('close', (c) => c === 0 ? resolve() : reject(new Error(`Exit ${c}`)));
        });
    }

    async stopScraper() {
        const cancelNames = [
            'scrape-discovery',
            'scrape-hotels-from-sitemap',
            'scrape-hotel-details-from-sitemap',
            'download-sitemap'
        ];

        let stopped = false;

        if (this.processCoordinator.scraperProcess) {
            this.processCoordinator.killProcess(this.processCoordinator.scraperProcess);
            stopped = true;
        }

        const jobs = await scrapeQueue.getJobs(['active', 'waiting', 'delayed']);
        for (const job of jobs) {
            if (!cancelNames.includes(job.name)) continue;
            const wasCancelled = await cancelJob(job.id);
            stopped = stopped || wasCancelled;
        }

        if (this.currentQueueJobId && !this.processCoordinator.scraperProcess) {
            // Best-effort local cleanup for single-process/dev setups.
            const localJob = activeRuns.get(this.currentQueueJobId);
            if (localJob?.pid) {
                this.processCoordinator.killProcess(localJob);
                stopped = true;
            }
        }

        if (stopped) {
            this.isRunning = false;
            this.startTime = null;
            this.currentQueueJobId = null;
            if (this.io) this.io.emit('status', { isRunning: false, startTime: null });
        }

        return stopped;
    }
    async stopPriceScraper() {
        let stopped = false;

        if (this.processCoordinator.priceScraperProcess) {
            this.processCoordinator.killProcess(this.processCoordinator.priceScraperProcess);
            stopped = true;
        }

        const jobs = await scrapeQueue.getJobs(['active', 'waiting', 'delayed']);
        for (const job of jobs) {
            if (!PRICE_JOB_NAMES.has(job.name)) continue;
            const wasCancelled = await cancelJob(job.id);
            stopped = stopped || wasCancelled;
        }

        if (stopped) {
            this.isPriceScraperRunning = false;
            this.priceScraperStartTime = null;
            if (this.io) this.io.emit('priceScraperStatus', { isRunning: false, startTime: null });
        }

        return stopped;
    }

    updateConfig(updates) {
        if (!this.isRunning) throw new Error('Not running');
        Object.assign(this.currentConfig, updates);
        if (this.io) this.io.emit('config', this.currentConfig);
        this.processCoordinator.sendConfigUpdate(updates, this.processCoordinator.scraperProcess, this.currentQueueJobId);
        return this.currentConfig;
    }

    async getQueueData() {
        const [active, waiting, completed, failed] = await Promise.all([
            scrapeQueue.getJobs(['active']),
            scrapeQueue.getJobs(['waiting', 'delayed']),
            scrapeQueue.getJobs(['completed'], 0, 10),
            scrapeQueue.getJobs(['failed'], 0, 10)
        ]);
        return { queue: [...active, ...waiting], history: [...completed, ...failed] };
    }

    // DEBUG HTML MANAGEMENT
    async listDebugHtmlFiles() {
        if (!fs.existsSync(DEBUG_HTML_DIR)) return [];

        const files = fs.readdirSync(DEBUG_HTML_DIR);
        const htmlFiles = files.filter(f => f.endsWith('.html') && !f.includes('.json'));

        const result = [];
        for (const file of htmlFiles) {
            const filePath = path.join(DEBUG_HTML_DIR, file);
            const jsonPath = filePath + '.json';
            let metadata = {};
            if (fs.existsSync(jsonPath)) {
                try {
                    metadata = JSON.parse(fs.readFileSync(jsonPath, 'utf-8'));
                } catch (err) {
                    console.error(`Failed to parse metadata for ${file}:`, err.message);
                }
            }
            const stats = fs.statSync(filePath);
            result.push({
                filename: file,
                size: stats.size,
                mtime: stats.mtime,
                ...metadata
            });
        }
        return result.sort((a, b) => b.mtime - a.mtime);
    }

    async getDebugHtmlFile(filename) {
        // Basic path traversal protection
        if (filename.includes('..') || !filename.endsWith('.html')) {
            throw new Error('Invalid filename');
        }
        const filePath = path.join(DEBUG_HTML_DIR, filename);
        if (!fs.existsSync(filePath)) {
            throw new Error('File not found');
        }
        return fs.readFileSync(filePath, 'utf-8');
    }

    // Required by controller (even if we'll add more granular ones)
    async debugHtmlDownload(config) {
        // For backwards compatibility or just listing
        return this.listDebugHtmlFiles();
    }
}

export const scraperManager = new ScraperManager();
