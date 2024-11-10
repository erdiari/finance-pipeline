#!/usr/bin/env python3

import asyncio
import json
import websockets
import redis
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from typing import Dict, List
import signal
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_ingest:binance")

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
                    CREATE TABLE IF NOT EXISTS trades_{symbol.lower()} (
                        time TIMESTAMPTZ NOT NULL,
                        source VARCHAR(50) NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        price DECIMAL NOT NULL,
                        quantity DECIMAL NOT NULL,
                        trade_id VARCHAR(100),
                        additional_data JSONB
                    );
                """)

                cur.execute("""
                    SELECT create_hypertable('trades', 'time',
                                        if_not_exists => TRUE);
                """)

                conn.commit()
            finally:
                cur.close()
                conn.close()

    def archive_symbol_trades(self, symbol: str, trades: List[str]) -> bool:
        """Archive all trades for a symbol to PostgreSQL"""
        if not trades:
            return True

        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor()

        try:
            values = []
            for trade in trades:
                trade_data = json.loads(trade)
                row = (
                    trade_data['timestamp'],
                    trade_data['source'],
                    trade_data['symbol'],
                    trade_data['price'],
                    trade_data['quantity'],
                    str(trade_data.get('trade_id', '')),
                    json.dumps({k: v for k, v in trade_data.items()
                              if k not in ['timestamp', 'source', 'symbol',
                                         'price', 'quantity', 'trade_id']})
                )
                values.append(row)

            execute_values(
                cur,
                """
                INSERT INTO trades (
                    time, source, symbol, price, quantity,
                    trade_id, additional_data
                )
                VALUES %s
                ON CONFLICT DO NOTHING
                """,
                values
            )

            conn.commit()
            logger.info(f"Archived {len(values)} trades for {symbol} to PostgreSQL")
            return True

        except Exception as e:
            logger.error(f"Error saving {symbol} to PostgreSQL: {e}")
            conn.rollback()
            return False
        finally:
            cur.close()
            conn.close()

class FinancialDataReader:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            decode_responses=True
        )

        self.running = True
        self.symbols = {'BTCUSDT', 'ETHUSDT', 'BNBUSDT'}

        # Archive configuration
        self.archive_interval = int(os.getenv('ARCHIVE_INTERVAL_SECONDS', 60))

        self.binance_ws_url = os.getenv(
            'BINANCE_WS_URL', 
            "wss://stream.binance.com:9443/ws"
        )

        self.db_manager = DatabaseManager(self.symbols)

    async def handle_binance_message(self, message: Dict):
        """Process Binance WebSocket message"""
        try:
            if 'e' in message and message['e'] == 'trade':
                data = {
                    'source': 'binance',
                    'symbol': message['s'],
                    'price': float(message['p']),
                    'quantity': float(message['q']),
                    'timestamp': datetime.fromtimestamp(message['T']/1000).isoformat(),
                    'trade_id': message['t'],
                    'buyer_maker': message['m']
                }

                # Store in Redis list using symbol as key
                list_key = f"trades:{data['symbol']}"
                json_data = json.dumps(data)
                self.redis_client.rpush(list_key, json_data)

                # logger.info(f"Binance trade: {data['symbol']} @ {data['price']}")

        except Exception as e:
            logger.error(f"Error processing Binance message: {e}")

    async def archive_data(self):
        """Periodically archive data to PostgreSQL and clear Redis lists"""
        while self.running:
            try:
                for symbol in self.symbols:
                    list_key = f"trades:{symbol}"

                    # Use Redis pipeline to get all trades and delete the list atomically
                    pipe = self.redis_client.pipeline()
                    pipe.lrange(list_key, 0, -1)  # Get all trades
                    pipe.delete(list_key)         # Delete the list
                    results = pipe.execute()

                    trades = results[0]  # trades from lrange

                    if trades:
                        # Archive to PostgreSQL
                        success = await asyncio.to_thread(
                            self.db_manager.archive_symbol_trades,
                            symbol,
                            trades
                        )

                        if not success:
                            # If archival failed, preserve the data by creating a new list
                            logger.warning(f"Archival failed for {symbol}, preserving data in Redis")
                            pipe = self.redis_client.pipeline()
                            for trade in trades:
                                pipe.rpush(list_key, trade)
                            pipe.execute()
                        else:
                            logger.info(f"Successfully archived and cleared trades for {symbol}")

            except Exception as e:
                logger.error(f"Error in archive_data: {e}")

            await asyncio.sleep(self.archive_interval)

    async def connect_binance(self):
        """Connect to Binance WebSocket"""
        streams = [f"{symbol.lower()}@trade" for symbol in self.symbols]
        ws_url = f"{self.binance_ws_url}/{''.join(streams)}"

        while self.running:
            try:
                async with websockets.connect(ws_url) as websocket:
                    logger.info("Connected to Binance WebSocket")

                    subscribe_message = {
                        "method": "SUBSCRIBE",
                        "params": streams,
                        "id": 1
                    }
                    await websocket.send(json.dumps(subscribe_message))

                    while self.running:
                        message = await websocket.recv()
                        await self.handle_binance_message(json.loads(message))

            except websockets.ConnectionClosed:
                logger.warning("Binance WebSocket connection closed, reconnecting...")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Binance WebSocket error: {e}")
                await asyncio.sleep(5)


    async def run(self):
        """Run all WebSocket connections concurrently"""
        tasks = [
            self.connect_binance(),
            self.archive_data()
        ]

        await asyncio.gather(*tasks)

def main():
    reader = FinancialDataReader()

    def shutdown(signum, frame):
        logger.info("Shutting down...")
        reader.running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    asyncio.run(reader.run())

if __name__ == "__main__":
    main()
