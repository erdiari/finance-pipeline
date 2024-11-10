#!/usr/bin/env python3
#
import asyncio
import json
import redis
import logging
from datetime import datetime, time, timedelta
import pytz
import psycopg2
from psycopg2.extras import execute_values
from typing import Dict, List, Optional
import signal
import aiohttp
import random
from dataclasses import dataclass
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_ingest:yahoo_finance")

@dataclass
class BackoffConfig:
    initial_delay: float = 1.0
    max_delay: float = 60.0
    factor: float = 2
    jitter: float = 0.1
    max_attempts: int = 5

    def calculate_delay(self, attempts: int) -> float:
        """Calculate delay with exponential backoff and jitter"""
        delay = min(self.max_delay, self.initial_delay * (self.factor ** attempts))
        jitter_amount = delay * self.jitter * random.uniform(-1, 1)
        return max(0, delay + jitter_amount)

class MarketHours:
    @staticmethod
    def is_market_open() -> bool:
        """Check if US market is currently open"""
        ny_tz = pytz.timezone('America/New_York')
        now = datetime.now(ny_tz)

        # Check if it's a weekday
        if now.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
            return False

        # Regular market hours (9:30 AM - 4:00 PM Eastern)
        market_open = time(9, 30)
        market_close = time(16, 0)

        current_time = now.time()
        return market_open <= current_time <= market_close

    @staticmethod
    def time_until_market_open() -> Optional[timedelta]:
        """Get time until market opens"""
        ny_tz = pytz.timezone('America/New_York')
        now = datetime.now(ny_tz)

        if MarketHours.is_market_open():
            return timedelta(0)

        # If it's after market close, calculate time until next day
        if now.time() > time(16, 0):
            next_day = now + timedelta(days=1)
        else:
            next_day = now

        # Find next weekday
        while next_day.weekday() >= 5:
            next_day += timedelta(days=1)

        next_market_open = datetime.combine(
            next_day.date(),
            time(9, 30),
            tzinfo=ny_tz
        )

        return next_market_open - now

class DatabaseManager:
    def __init__(self, symbols):
        self.db_config = {
            'dbname': os.getenv('POSTGRES_DB', 'timeseries_db'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', 'postgres'), 
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432')
        }

        self.symbols = symbols
        self.init_database()

    def init_database(self):
        """Initialize the PostgreSQL database with TimescaleDB"""
        for symbol in self.symbols:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()

            try:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS yahoo_data_{symbol.lower()} (
                        time TIMESTAMPTZ NOT NULL,
                        price DECIMAL NOT NULL,
                        volume DECIMAL,
                        open DECIMAL,
                        high DECIMAL,
                        low DECIMAL,
                        additional_data JSONB
                    );
                """)

                cur.execute(f"""
                    SELECT create_hypertable('yahoo_data_{symbol.lower()}', 'time',
                                        if_not_exists => TRUE);
                """)

                # Create indexes
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_yahoo_symbol_time
                    ON yahoo_data_{symbol.lower()} (time DESC);
                """)

                conn.commit()
                logger.info(f"Database initialized successfully for {symbol}")

            except Exception as e:
                logger.error(f"Error initializing database for {symbol}: {e}")
                raise
            finally:
                cur.close()
                conn.close()

    def get_table_name(self, symbol: str) -> str:
        """Get the table name for a symbol"""
        return f"yahoo_data_{symbol.lower()}"

    def archive_market_data(self, market_data: List[str]) -> bool:
        """Archive market data to PostgreSQL symbol-specific tables"""
        if not market_data:
            return True

        # Group data by symbol
        symbol_data = {}
        for data_str in market_data:
            data = json.loads(data_str)
            symbol = data['symbol']
            if symbol not in symbol_data:
                symbol_data[symbol] = []
            symbol_data[symbol].append(data)

        # Archive data for each symbol to its specific table
        success = True
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor()

        try:
            for symbol, data_list in symbol_data.items():
                table_name = self.get_table_name(symbol)
                values = [
                    (
                        data['timestamp'],
                        data['price'],
                        data.get('volume', 0),
                        data['additional'].get('open'),
                        data['additional'].get('high'),
                        data['additional'].get('low'),
                        json.dumps(data.get('additional', {}))
                    )
                    for data in data_list
                ]

                try:
                    execute_values(
                        cur,
                        f"""
                        INSERT INTO {table_name} (
                            time, price, volume,
                            open, high, low, additional_data
                        )
                        VALUES %s
                        ON CONFLICT DO NOTHING
                        """,
                        values
                    )

                    conn.commit()
                    logger.info(f"Archived {len(values)} records for {symbol}")

                except Exception as e:
                    logger.error(f"Error archiving data for {symbol}: {e}")
                    conn.rollback()
                    success = False

        finally:
            cur.close()
            conn.close()

        return success

class YahooFinanceCollector:
    def __init__(self):

        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            decode_responses=True
        )

        self.session: Optional[aiohttp.ClientSession] = None
        self.backoff = BackoffConfig()
        self.db_manager = DatabaseManager(self.symbols)

        self.base_url = "https://query1.finance.yahoo.com/v8/finance/chart/"
        self.running = True

        # Configuration
        self.poll_interval = int(os.getenv('POLL_INTERVAL', 60))
        self.archive_interval = int(os.getenv('ARCHIVE_INTERVAL', 300))
        self.symbols = set(os.getenv('SYMBOLS', 'AAPL,MSFT,GOOGL').split(','))

    async def ensure_session(self):
        """Ensure aiohttp session exists"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()

    async def close(self):
        """Close the session"""
        if self.session and not self.session.closed:
            await self.session.close()


    async def process_market_data(self, symbol: str, data: Dict):
        """Process and store market data"""
        try:
            if not data or 'chart' not in data or not data['chart']['result']:
                logger.warning(f"No data returned for {symbol}")
                return

            chart = data['chart']['result'][0]

            # Check if we have any data points
            if not chart.get('timestamp'):
                logger.warning(f"No timestamps in data for {symbol}")
                return

            timestamps = chart['timestamp']
            indicators = chart['indicators']['quote'][0]

            # Validate we have all required data
            if not all(key in indicators for key in ['close', 'volume', 'open', 'high', 'low']):
                logger.warning(f"Missing required indicators for {symbol}")
                return

            # Get the latest data point
            last_idx = -1

            # Validate the last data point
            if (indicators['close'][last_idx] is None or
                indicators['volume'][last_idx] is None):
                logger.warning(f"Invalid last data point for {symbol}")
                return

            market_data = {
                'symbol': symbol,
                'price': float(indicators['close'][last_idx]),
                'volume': float(indicators['volume'][last_idx]),
                'timestamp': datetime.fromtimestamp(timestamps[last_idx]).isoformat(),
                'additional': {
                    'open': float(indicators['open'][last_idx]) if indicators['open'][last_idx] is not None else None,
                    'high': float(indicators['high'][last_idx]) if indicators['high'][last_idx] is not None else None,
                    'low': float(indicators['low'][last_idx]) if indicators['low'][last_idx] is not None else None,
                    'timestamp_epoch': timestamps[last_idx]
                }
            }

            # Store in Redis
            list_key = f"yahoo:{symbol}"
            json_data = json.dumps(market_data)
            self.redis_client.rpush(list_key, json_data)

            logger.info(f"Stored {symbol} @ {market_data['price']}")

        except (KeyError, IndexError, TypeError) as e:
            logger.error(f"Error processing data for {symbol}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error processing {symbol}: {e}")

    async def collect_symbol_data(self, symbol: str):
        """Collect data for a single symbol with retries"""
        try:
            data = await self.fetch_market_data(symbol)
            if data:
                await self.process_market_data(symbol, data)
        except Exception as e:
            logger.error(f"Error collecting data for {symbol}: {e}")

    async def collect_data(self):
        """Collect market data for all symbols with rate limiting"""
        while self.running:
            try:
                if not MarketHours.is_market_open():
                    wait_time = MarketHours.time_until_market_open()
                    logger.info(f"Market is closed. Waiting {wait_time} until market opens")
                    await asyncio.sleep(min(wait_time.total_seconds(), 300))  # Check every 5 minutes max
                    continue

                # Process symbols in batches to avoid rate limiting
                for i in range(0, len(self.symbols), self.concurrent_requests):
                    batch = list(self.symbols)[i:i + self.concurrent_requests]
                    tasks = [self.collect_symbol_data(symbol) for symbol in batch]
                    await asyncio.gather(*tasks)

                    # Add delay between batches
                    if i + self.concurrent_requests < len(self.symbols):
                        await asyncio.sleep(2)  # 2 second delay between batches

            except Exception as e:
                logger.error(f"Error in collect_data: {e}")

            await asyncio.sleep(self.poll_interval)

    async def fetch_market_data(self, symbol: str) -> Optional[Dict]:
        """Fetch data from Yahoo Finance with improved error handling"""
        await self.ensure_session()
        attempts = 0

        while attempts < self.backoff.max_attempts:
            try:
                params = {
                    'interval': '1m',
                    'range': '1d',
                    'includePrePost': 'false'  # Only regular market hours
                }

                async with self.session.get(
                    f"{self.base_url}{symbol}",
                    params=params,
                    timeout=30  # 30 second timeout
                ) as response:
                    if response.status == 200:
                        return await response.json()

                    elif response.status == 429:  # Too Many Requests
                        delay = self.backoff.calculate_delay(attempts)
                        logger.warning(f"Rate limited for {symbol}. Waiting {delay:.2f}s")
                        await asyncio.sleep(delay)

                    else:
                        delay = self.backoff.calculate_delay(attempts)
                        logger.error(f"HTTP {response.status} for {symbol}. Waiting {delay:.2f}s")
                        await asyncio.sleep(delay)

            except asyncio.TimeoutError:
                logger.error(f"Timeout fetching data for {symbol}")
                delay = self.backoff.calculate_delay(attempts)
                await asyncio.sleep(delay)
            except aiohttp.ClientError as e:
                delay = self.backoff.calculate_delay(attempts)
                logger.error(f"Request error for {symbol}: {e}. Waiting {delay:.2f}s")
                await asyncio.sleep(delay)

            attempts += 1

        logger.error(f"Max retry attempts reached for {symbol}")
        return None

    async def archive_data(self):
        """Periodically archive data to PostgreSQL"""
        while self.running:
            try:
                for symbol in self.symbols:
                    list_key = f"yahoo:{symbol}"

                    # Get all data and delete the list atomically
                    pipe = self.redis_client.pipeline()
                    pipe.lrange(list_key, 0, -1)
                    pipe.delete(list_key)
                    results = pipe.execute()

                    market_data = results[0]

                    if market_data:
                        success = await asyncio.to_thread(
                            self.db_manager.archive_market_data,
                            market_data
                        )

                        if not success:
                            # Restore data if archival failed
                            logger.warning(f"Archival failed for {symbol}, restoring data")
                            pipe = self.redis_client.pipeline()
                            for data in market_data:
                                pipe.rpush(list_key, data)
                            pipe.execute()

            except Exception as e:
                logger.error(f"Error in archive_data: {e}")

            await asyncio.sleep(self.archive_interval)

    async def collect_data(self):
        """Collect market data for all symbols"""
        while self.running:
            try:
                tasks = []
                for symbol in self.symbols:
                    data = await self.fetch_market_data(symbol)
                    if data:
                        tasks.append(self.process_market_data(symbol, data))

                if tasks:
                    await asyncio.gather(*tasks)

            except Exception as e:
                logger.error(f"Error in collect_data: {e}")

            await asyncio.sleep(self.poll_interval)

    async def run(self):
        """Run data collection and archival tasks"""
        try:
            await asyncio.gather(
                self.collect_data(),
                self.archive_data()
            )
        finally:
            await self.close()

def main():
    collector = YahooFinanceCollector()

    def shutdown(signum, frame):
        logger.info("Shutting down...")
        collector.running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    asyncio.run(collector.run())

if __name__ == "__main__":
    main()
