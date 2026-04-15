#!/usr/bin/env python3
"""
Sitemap Downloader for PostgreSQL
Adapted from booking.py to save hotel URLs directly to PostgreSQL
with optional SQLite/CSV export
"""

import argparse
import asyncio
import aiohttp
from aiohttp_socks import ProxyConnector
import xml.etree.ElementTree as ET
import gzip
import time
import os
import sys
import logging
import psycopg2
from psycopg2 import pool
from urllib.parse import urlparse
from typing import List, Set, Optional, AsyncIterator
from dataclasses import dataclass
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import re
import json
import csv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class URLData:
    """Hotel URL data structure"""
    url: str
    lastmod: Optional[str] = None
    sitemap_source: str = ""
    sitemap_path: str = ""
    depth_level: int = 0

class PostgresConnectionPool:
    """PostgreSQL connection pool"""

    def __init__(self, db_config: dict, max_connections: int = 20):
        # Retry logic for database connection
        max_retries = 30
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                self.pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=5,
                    maxconn=max_connections,
                    **db_config
                )
                logger.info(f"PostgreSQL pool created with {max_connections} max connections")
                break
            except psycopg2.OperationalError as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Database connection failed (attempt {attempt + 1}/{max_retries}): {e}")
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to connect to database after {max_retries} attempts")
                    raise
    
    def get_connection(self):
        return self.pool.getconn()
    
    def return_connection(self, conn):
        self.pool.putconn(conn)
    
    def close_all(self):
        self.pool.closeall()

class ProxyRotator:
    """Simple proxy rotator"""
    def __init__(self, proxy_file: Optional[str] = None):
        self.proxies = []
        self.current_index = 0
        if proxy_file and os.path.exists(proxy_file):
            try:
                with open(proxy_file, 'r') as f:
                    raw_proxies = [line.strip() for line in f if line.strip() and not line.startswith('#')]
                    self.proxies = [self._format_proxy(p) for p in raw_proxies]
                logger.info(f"🔄 Loaded {len(self.proxies)} proxies from {proxy_file}")
            except Exception as e:
                logger.error(f"❌ Error loading proxies: {e}")

    def _format_proxy(self, proxy: str) -> str:
        """Format proxy string to http://user:pass@host:port"""
        if proxy.startswith('http://') or proxy.startswith('https://'):
            return proxy
        
        parts = proxy.split(':')
        if len(parts) == 4:
            # host:port:user:pass -> http://user:pass@host:port
            return f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
        elif len(parts) == 2:
            # host:port -> http://host:port
            return f"http://{parts[0]}:{parts[1]}"
        
        # Fallback: assume http:// prefix needed
        return f"http://{proxy}"

    def get_proxy(self) -> Optional[str]:
        if not self.proxies:
            return None
        proxy = self.proxies[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.proxies)
        return proxy

class SitemapDownloader:
    def __init__(self, db_config: dict, max_concurrent: int = 100, country_code: Optional[str] = None,
                 export_sqlite: bool = False, export_csv: bool = False, proxy_file: Optional[str] = None,
                 use_proxy_chain: bool = True, socks5_proxy: str = '127.0.0.1:1080'):
        """
        Sitemap downloader with PostgreSQL storage

        Args:
            db_config: PostgreSQL connection config
            max_concurrent: Max concurrent downloads
            country_code: Country filter (e.g., 'it', 'fr')
            export_sqlite: Also export to SQLite
            export_csv: Also export to CSV
            proxy_file: Path to proxy file
            use_proxy_chain: Use SOCKS5 proxy chain (default: True)
            socks5_proxy: SOCKS5 proxy address (default: 127.0.0.1:1080)
        """
        self.country_code = country_code.lower().strip() if country_code else None
        # Optimize for maximum performance: 500 concurrent downloads (up from 200)
        self.max_concurrent = min(max_concurrent, 500)
        self.export_sqlite = export_sqlite
        self.export_csv = export_csv
        self.processed_urls: Set[str] = set()
        self.pending_urls: Set[str] = set()

        # Proxy configuration
        self.use_proxy_chain = use_proxy_chain
        self.socks5_proxy = socks5_proxy
        self.proxy_rotator = ProxyRotator(proxy_file)

        # CPU count
        self.cpu_count = multiprocessing.cpu_count()

        # Optimized PostgreSQL pool: more connections for higher throughput
        self.db_pool = PostgresConnectionPool(db_config, max_connections=min(50, self.cpu_count * 4))

        # Optimized Thread pool: more workers for parallel decompression/parsing
        self.thread_pool = ThreadPoolExecutor(max_workers=self.cpu_count * 4)

        # Optimized Semaphores: higher limits for better parallelization
        self.download_semaphore = asyncio.Semaphore(self.max_concurrent)
        self.db_semaphore = asyncio.Semaphore(min(30, self.cpu_count * 2))

        # Stats
        self.stats = {
            'downloaded_sitemaps': 0,
            'saved_urls': 0,
            'errors': 0,
            'start_time': time.time()
        }

        # Optimized Progress tracking
        self.total_sitemaps_expected = 0
        self.completed_sitemaps = 0
        self._all_sitemaps: Set[str] = set()
        self.save_queue: Optional[asyncio.Queue] = None
        self.writer_tasks: List[asyncio.Task] = []
        # Optimized: more writer workers for faster DB inserts
        self.writer_count = max(4, min(12, self.cpu_count * 2))
        # Optimized: larger batches for better DB throughput
        self.batch_size_urls = 2000
        
        # Initialize database
        self.init_database()
        
        # Initialize CSV if needed
        if self.export_csv:
            self.csv_file = open(f'../../data/hotel_urls_{self.country_code or "all"}.csv', 'w', newline='', encoding='utf-8')
            self.csv_writer = csv.writer(self.csv_file)
            self.csv_writer.writerow(['url', 'lastmod', 'sitemap_source', 'depth_level'])
        
        logger.info(f"🚀 Sitemap Downloader: {self.max_concurrent} concurrent, {self.cpu_count} CPUs")
        if self.country_code:
            logger.info(f"🌍 Country filter: {self.country_code.upper()}")
        if self.use_proxy_chain:
            logger.info(f"🔗 Proxy chain enabled: SOCKS5 ({self.socks5_proxy}) → Webshare")
        if self.export_sqlite:
            logger.info("💾 SQLite export enabled")
        if self.export_csv:
            logger.info("📄 CSV export enabled")
    
    def init_database(self):
        """Initialize PostgreSQL tables"""
        conn = self.db_pool.get_connection()
        try:
            cursor = conn.cursor()
            
            # Create sitemap_urls table if not exists
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sitemap_urls (
                    id BIGSERIAL PRIMARY KEY,
                    url TEXT UNIQUE NOT NULL,
                    domain TEXT,
                    path TEXT,
                    lastmod TEXT,
                    sitemap_source TEXT,
                    sitemap_path TEXT,
                    depth_level INTEGER DEFAULT 0,
                    country_code TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            ''')
            
            # Add columns if they don't exist (migration for existing tables)
            cursor.execute('ALTER TABLE sitemap_urls ADD COLUMN IF NOT EXISTS domain TEXT')
            cursor.execute('ALTER TABLE sitemap_urls ADD COLUMN IF NOT EXISTS path TEXT')
            cursor.execute('ALTER TABLE sitemap_urls ADD COLUMN IF NOT EXISTS country_code TEXT')
            cursor.execute('ALTER TABLE sitemap_urls ADD COLUMN IF NOT EXISTS sitemap_source TEXT')
            cursor.execute('ALTER TABLE sitemap_urls ADD COLUMN IF NOT EXISTS sitemap_path TEXT')
            
            # Indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sitemap_urls_url ON sitemap_urls(url)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sitemap_urls_country ON sitemap_urls(country_code)')
            
            # Processed sitemaps tracking
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processed_sitemaps (
                    sitemap_url TEXT PRIMARY KEY,
                    processed_at TIMESTAMPTZ DEFAULT NOW(),
                    url_count INTEGER DEFAULT 0
                )
            ''')
            
            conn.commit()
            logger.info("✅ PostgreSQL tables initialized")
            
        finally:
            self.db_pool.return_connection(conn)
    
    def shutdown(self):
        """Cleanup resources"""
        try:
            self.thread_pool.shutdown(wait=True)
        finally:
            self.db_pool.close_all()
            if self.export_csv:
                self.csv_file.close()
    
    async def start_writer_pool(self):
        """Start async writer workers"""
        if self.writer_tasks:
            return
        if self.save_queue is None:
            # Optimized: larger queue size (1000) to reduce backpressure
            self.save_queue = asyncio.Queue(maxsize=1000)
        for idx in range(self.writer_count):
            task = asyncio.create_task(self._writer_worker(idx))
            self.writer_tasks.append(task)
        logger.info(f"🧵 Writer pool started with {len(self.writer_tasks)} workers")
    
    async def stop_writer_pool(self):
        """Stop writer pool"""
        if not self.writer_tasks:
            return
        if self.save_queue is not None:
            for _ in self.writer_tasks:
                await self.save_queue.put(None)
            await self.save_queue.join()
        for task in self.writer_tasks:
            try:
                await task
            except Exception as exc:
                logger.warning(f"Writer worker error: {exc}")
        self.writer_tasks.clear()
        self.save_queue = None
    
    async def _writer_worker(self, worker_id: int):
        """Writer worker coroutine"""
        loop = asyncio.get_running_loop()
        while True:
            if self.save_queue is None:
                break
            item = await self.save_queue.get()
            if item is None:
                self.save_queue.task_done()
                break
            urls, future = item
            try:
                result = await loop.run_in_executor(self.thread_pool, self._save_urls_sync, urls)
                if not future.done():
                    future.set_result(result)
            except Exception as exc:
                logger.error(f"Writer {worker_id} error: {exc}")
                if not future.done():
                    future.set_exception(exc)
            finally:
                self.save_queue.task_done()
    
    @asynccontextmanager
    async def http_session(self):
        """HTTP session context manager with optional SOCKS5 proxy chain - OPTIMIZED"""
        timeout = aiohttp.ClientTimeout(total=60, connect=20)

        # Use SOCKS5 proxy chain if enabled
        if self.use_proxy_chain:
            logger.info(f"🔗 Using SOCKS5 proxy chain: {self.socks5_proxy}")
            # Optimized ProxyConnector with higher limits for better throughput
            connector = ProxyConnector.from_url(
                f'socks5://{self.socks5_proxy}',
                limit=600,  # Total connections
                limit_per_host=100,  # Per host limit
                ttl_dns_cache=600,
                use_dns_cache=True,
                ssl=False
            )
        else:
            # Optimized TCP Connector
            connector = aiohttp.TCPConnector(
                limit=600,  # Increased from 300
                limit_per_host=100,  # Increased from 50
                ttl_dns_cache=600,
                use_dns_cache=True,
                ssl=False
            )

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Encoding': 'gzip, deflate, br'
        }

        async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers) as session:
            yield session
    
    async def download_xml_async(self, session: aiohttp.ClientSession, url: str) -> Optional[ET.Element]:
        """Download and parse XML sitemap through SOCKS5 proxy (NO Webshare to avoid AWS WAF)"""
        async with self.download_semaphore:
            try:
                # NOTE: We DON'T use Webshare for sitemap downloads because:
                # 1. Webshare IPs are flagged by AWS WAF (Status 202 HTML challenge)
                # 2. Sitemap downloads work fine through SOCKS5 (Shadowsocks → UNIGE → Internet)
                # 3. Hotel scraper uses Webshare + token harvesting to bypass WAF, but sitemaps don't need it

                # When using proxy chain:
                # - ProxyConnector already routes through SOCKS5 (set in http_session)
                # - SOCKS5 → Shadowsocks → UNIGE → Booking.com
                # - NO Webshare layer for sitemaps (to avoid AWS WAF blocking)
                async with session.get(url) as response:
                    response.raise_for_status()

                    content = b""
                    # Optimized: 128KB chunks (doubled from 64KB) for faster downloads
                    async for chunk in response.content.iter_chunked(131072):
                        content += chunk
                        if len(content) > 100 * 1024 * 1024:  # 100MB limit
                            logger.warning(f"File too large: {url}")
                            return None

                # Decompress if needed
                if url.endswith('.gz'):
                    content = await asyncio.get_event_loop().run_in_executor(
                        self.thread_pool, gzip.decompress, content
                    )

                # Parse XML
                root = await asyncio.get_event_loop().run_in_executor(
                    self.thread_pool, ET.fromstring, content
                )

                self.stats['downloaded_sitemaps'] += 1
                return root

            except Exception as e:
                self.stats['errors'] += 1
                logger.error(f"❌ Error downloading {url}: {e}")
                return None
    
    def is_hotel_url(self, url: str) -> bool:
        """Check if URL is a hotel page and matches country filter"""
        if '/hotel/' not in url:
            return False
        
        try:
            parsed = urlparse(url)
            path = parsed.path.lower()
            
            if not path.endswith('.html'):
                return False
            
            # Country filter
            if self.country_code:
                hotel_country_match = re.search(r'/hotel/([a-z]{2})/', path)
                if not hotel_country_match:
                    # logger.debug(f"Rejected (no country in path): {url}")
                    return False
                hotel_country = hotel_country_match.group(1)
                if hotel_country != self.country_code:
                    # logger.debug(f"Rejected (country mismatch {hotel_country}!={self.country_code}): {url}")
                    return False
            
            return True
        except:
            return False
    
    async def save_urls_batch_async(self, urls: List[URLData]) -> Optional[asyncio.Future]:
        """Queue batch for async saving"""
        if not urls:
            return None
        
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        if self.save_queue is None:
            # Optimized: larger queue size (1000) to reduce backpressure
            self.save_queue = asyncio.Queue(maxsize=1000)
        await self.save_queue.put((urls, future))
        return future
    
    def _save_urls_sync(self, urls: List[URLData]) -> int:
        """Save URLs to PostgreSQL (and optionally SQLite/CSV)"""
        conn = self.db_pool.get_connection()
        try:
            cursor = conn.cursor()
            
            # Prepare batch data
            batch_data = []
            for url_data in urls:
                try:
                    parsed_url = urlparse(url_data.url)
                    batch_data.append((
                        url_data.url,
                        parsed_url.netloc,
                        parsed_url.path,
                        url_data.lastmod,
                        url_data.sitemap_source,
                        url_data.sitemap_path,
                        url_data.depth_level,
                        self.country_code
                    ))
                except Exception:
                    continue
            
            if not batch_data:
                return 0
            
            # Insert into PostgreSQL
            insert_query = '''
                INSERT INTO sitemap_urls (url, domain, path, lastmod, sitemap_source, sitemap_path, depth_level, country_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING
            '''
            
            cursor.executemany(insert_query, batch_data)
            saved_count = cursor.rowcount
            conn.commit()
            
            # Export to CSV if enabled
            if self.export_csv:
                for url_data in urls:
                    self.csv_writer.writerow([
                        url_data.url,
                        url_data.lastmod,
                        url_data.sitemap_source,
                        url_data.depth_level
                    ])
            
            self.stats['saved_urls'] += saved_count
            return saved_count
            
        except Exception as e:
            logger.error(f"Error saving batch: {e}")
            conn.rollback()
            return 0
        finally:
            self.db_pool.return_connection(conn)
    
    async def process_sitemap_urls_stream(self, root: ET.Element, sitemap_info: dict) -> AsyncIterator[List[URLData]]:
        """Stream URLs from sitemap in batches"""
        batch = []
        
        for url_elem in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
            loc_elem = url_elem.find('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
            if loc_elem is None:
                continue
            
            url = loc_elem.text.strip()
            if not self.is_hotel_url(url):
                continue
            
            # Enforce .it.html suffix for Italian content
            # Remove existing language suffix if present (e.g., .fr.html, .en-gb.html) or just .html
            # Regex to match the end of the URL: .html or .xx.html or .xx-xx.html
            url = re.sub(r'(\.[a-z]{2}(-[a-z]{2})?)?\.html$', '.it.html', url)
            
            lastmod_elem = url_elem.find('.//{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod')
            
            url_data = URLData(
                url=url,
                lastmod=lastmod_elem.text.strip() if lastmod_elem is not None else None,
                sitemap_source=sitemap_info['source'],
                sitemap_path=sitemap_info['path'],
                depth_level=sitemap_info['depth']
            )
            
            batch.append(url_data)
            
            if len(batch) >= self.batch_size_urls:
                yield batch
                batch = []
        
        if batch:
            yield batch
    
    async def process_single_sitemap(self, session: aiohttp.ClientSession, sitemap_url: str,
                                   parent_path: str = "", depth: int = 0) -> int:
        """Process a single sitemap recursively"""
        if sitemap_url in self.processed_urls or sitemap_url in self.pending_urls:
            return 0
        
        self.pending_urls.add(sitemap_url)
        
        try:
            root = await self.download_xml_async(session, sitemap_url)
            if root is None:
                return 0
            
            current_path = f"{parent_path} → {sitemap_url}" if parent_path else sitemap_url
            total_saved = 0
            
            # Sitemap index
            if root.tag.endswith('sitemapindex'):
                logger.info(f"📋 Processing sitemap index - depth {depth}")
                
                child_sitemaps = []
                for sitemap in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap'):
                    loc_elem = sitemap.find('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
                    if loc_elem is not None and loc_elem.text:
                        child_url = loc_elem.text.strip()
                        if child_url and child_url not in self.processed_urls:
                            child_sitemaps.append(child_url)
                            self._all_sitemaps.add(child_url)
                
                self.total_sitemaps_expected = len(self._all_sitemaps)
                
                # Process children in batches
                batch_size = min(20, self.max_concurrent // 3)
                for i in range(0, len(child_sitemaps), batch_size):
                    batch = child_sitemaps[i:i + batch_size]
                    tasks = [
                        self.process_single_sitemap(session, child_url, current_path, depth + 1)
                        for child_url in batch
                    ]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for result in results:
                        if isinstance(result, int):
                            total_saved += result
            
            # Sitemap urlset
            elif root.tag.endswith('urlset'):
                logger.info(f"🏨 Processing hotel sitemap - depth {depth}")
                
                sitemap_info = {
                    'source': sitemap_url,
                    'path': current_path,
                    'depth': depth
                }
                
                # Save batches
                save_futures = []
                async for batch_urls in self.process_sitemap_urls_stream(root, sitemap_info):
                    future = await self.save_urls_batch_async(batch_urls)
                    if future:
                        save_futures.append(future)
                
                # Wait for all saves
                results = await asyncio.gather(*save_futures, return_exceptions=True) if save_futures else []
                total_saved = sum(r for r in results if isinstance(r, int))
                
                logger.info(f"💾 Saved {total_saved} hotels from {sitemap_url}")
            
            self.processed_urls.add(sitemap_url)
            self.pending_urls.discard(sitemap_url)
            
            if sitemap_url in self._all_sitemaps:
                self.completed_sitemaps += 1
                progress = (self.completed_sitemaps / self.total_sitemaps_expected * 100) if self.total_sitemaps_expected else 0
                logger.info(f"📦 Progress: {self.completed_sitemaps}/{self.total_sitemaps_expected} ({progress:.1f}%) | Saved: {self.stats['saved_urls']}")
            
            return total_saved
            
        except Exception as e:
            logger.error(f"❌ Error processing {sitemap_url}: {e}")
            self.pending_urls.discard(sitemap_url)
            return 0
    
    async def download_sitemaps(self):
        """Main download process"""
        sitemap_url = "https://www.booking.com/sitembk-hotel-index.xml"
        
        logger.info(f"🚀 Starting sitemap download")
        logger.info(f"💾 Max concurrent: {self.max_concurrent}")
        logger.info(f"🧠 CPU cores: {self.cpu_count}")
        
        try:
            async with self.http_session() as session:
                await self.start_writer_pool()
                try:
                    total_saved = await self.process_single_sitemap(session, sitemap_url)
                finally:
                    await self.stop_writer_pool()
                
                elapsed = time.time() - self.stats['start_time']
                logger.info(f"🎉 Download completed!")
                logger.info(f"📊 Stats: {total_saved:,} hotels in {elapsed:.1f}s ({total_saved/elapsed:.1f} hotels/sec)")
                logger.info(f"❌ Errors: {self.stats['errors']}")
                
                return total_saved
                
        except Exception as e:
            logger.error(f"❌ Critical error: {e}")
            raise
        finally:
            self.shutdown()

async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Sitemap Downloader for PostgreSQL")
    parser.add_argument("--db-host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--db-port", default="5432", help="PostgreSQL port")
    parser.add_argument("--db-name", default="scraper_db", help="PostgreSQL database")
    parser.add_argument("--db-user", default="user", help="PostgreSQL user")
    parser.add_argument("--db-pass", default="password", help="PostgreSQL password")
    parser.add_argument("--country", type=str, help="Country filter (e.g., it, fr, us)")
    parser.add_argument("--max-concurrent", type=int, default=100, help="Max concurrent downloads")
    parser.add_argument("--export-sqlite", action="store_true", help="Also export to SQLite")
    parser.add_argument("--export-csv", action="store_true", help="Also export to CSV")
    parser.add_argument("--proxy-file", type=str, help="Path to proxy file")
    parser.add_argument("--use-proxy-chain", action="store_true", default=True, help="Use SOCKS5 proxy chain (default: True)")
    parser.add_argument("--no-proxy-chain", dest="use_proxy_chain", action="store_false", help="Disable SOCKS5 proxy chain")
    parser.add_argument("--socks5-proxy", type=str, default="127.0.0.1:1080", help="SOCKS5 proxy address (default: 127.0.0.1:1080)")
    args = parser.parse_args()

    db_config = {
        'host': args.db_host,
        'port': args.db_port,
        'database': args.db_name,
        'user': args.db_user,
        'password': args.db_pass
    }

    downloader = SitemapDownloader(
        db_config=db_config,
        max_concurrent=args.max_concurrent,
        country_code=args.country,
        export_sqlite=args.export_sqlite,
        export_csv=args.export_csv,
        proxy_file=args.proxy_file,
        use_proxy_chain=args.use_proxy_chain,
        socks5_proxy=args.socks5_proxy
    )
    
    await downloader.download_sitemaps()

if __name__ == "__main__":
    asyncio.run(main())
