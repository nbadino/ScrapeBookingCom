FROM oven/bun:1

WORKDIR /app

# Install Python3, pip, curl for NodeSource, and Playwright dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    build-essential \
    curl \
    ca-certificates \
    gnupg \
    # Playwright system deps
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js 20 from NodeSource (needed for Playwright - Bun has pipe communication issues)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies for sitemap downloader (system-wide for simplicity in Docker)
RUN pip3 install aiohttp aiohttp-socks python-socks[asyncio] psycopg2-binary brotli --break-system-packages

# Copy package files
COPY package*.json ./
COPY apps/api/package*.json ./apps/api/
COPY apps/scraper/package*.json ./apps/scraper/

# Install dependencies with Bun (for running the API server)
RUN bun install

# Also install with npm so Node.js can resolve modules for Playwright scripts
# (Bun's .bun module structure is incompatible with Node's ESM resolution)
RUN npm install --legacy-peer-deps

# Set Playwright browser path
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Install Playwright browsers with ALL dependencies (use npx since npm installed it)
RUN npx playwright install --with-deps chromium

# Copy source code (respects .dockerignore)
WORKDIR /app
COPY . .

# Ensure Go binary is included and executable
COPY apps/api/bin/booking-scraper /app/apps/api/bin/booking-scraper
RUN chmod +x /app/apps/api/bin/booking-scraper

# Expose port
EXPOSE 3000

# Start server
WORKDIR /app/apps/api
CMD ["bun", "run", "start"]

