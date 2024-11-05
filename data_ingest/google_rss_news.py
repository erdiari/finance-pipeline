#/usr/bin/env python3

import asyncio
import aiohttp
import feedparser
import logging
from datetime import datetime, timezone, timedelta
import motor.motor_asyncio
from typing import Dict, List, Optional
import signal
from dataclasses import dataclass
import random
import hashlib
from urllib.parse import quote, urlparse
from time import mktime
from pymongo import UpdateOne

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('data_ingest:bloomberg')

@dataclass
class BackoffConfig:
    initial_delay: float = 1.0
    max_delay: float = 60.0
    factor: float = 2
    jitter: float = 0.1
    max_attempts: int = 5

    def calculate_delay(self, attempts: int) -> float:
        delay = min(self.max_delay, self.initial_delay * (self.factor ** attempts))
        jitter_amount = delay * self.jitter * random.uniform(-1, 1)
        return max(0, delay + jitter_amount)

class NewsFeeds:
    """Financial news feed URLs"""

    @staticmethod
    def get_company_feed(company: str) -> str:
        """Get RSS feed URL for a company"""
        encoded_company = quote(company)
        return f"https://news.google.com/rss/search?q={encoded_company}+stock&hl=en-US&gl=US&ceid=US:en"

    @staticmethod
    def get_stock_feed(symbol: str) -> str:
        """Get RSS feed URL for a stock symbol"""
        return f"https://news.google.com/rss/search?q=stock+{symbol}+price&hl=en-US&gl=US&ceid=US:en"

    COMPANIES = {
        'AAPL': 'Apple',
        'MSFT': 'Microsoft',
        'GOOGL': 'Google',
        'AMZN': 'Amazon',
        'META': 'Meta',
        'NVDA': 'NVIDIA',
        'TSLA': 'Tesla',
        'JPM': 'JPMorgan Chase',
        'V': 'Visa',
        'NFLX': 'Netflix',
        'BAC': 'Bank of America',
        'WMT': 'Walmart',
        'XOM': 'Exxon Mobil',
        'JNJ': 'Johnson & Johnson',
        'MA': 'Mastercard'
    }

    TOPICS = {
        'business': 'https://news.google.com/rss/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx6TVdZU0FtVnVHZ0pWVXlnQVAB',
        'technology': 'https://news.google.com/rss/topics/CAAqJggKIiBDQkFTRWdvSUwyMHZNRGRqTVhZU0FtVnVHZ0pWVXlnQVAB',
        'markets': 'https://news.google.com/rss/search?q=stock+market+trading&hl=en-US&gl=US&ceid=US:en',
        'crypto': 'https://news.google.com/rss/search?q=cryptocurrency+bitcoin+ethereum&hl=en-US&gl=US&ceid=US:en',
        'earnings': 'https://news.google.com/rss/search?q=quarterly+earnings+results&hl=en-US&gl=US&ceid=US:en',
        'ipo': 'https://news.google.com/rss/search?q=ipo+stock+market&hl=en-US&gl=US&ceid=US:en',
        'forex': 'https://news.google.com/rss/search?q=forex+currency+trading&hl=en-US&gl=US&ceid=US:en',
        'commodities': 'https://news.google.com/rss/search?q=commodities+trading+gold+oil&hl=en-US&gl=US&ceid=US:en'
    }

    @classmethod
    def get_all_feeds(cls) -> Dict[str, str]:
        """Get all feeds including companies and topics"""
        feeds = {}

        # Add topic feeds
        feeds.update(cls.TOPICS)

        # Add company-specific feeds
        for symbol, company in cls.COMPANIES.items():
            feeds[f"{symbol}_news"] = cls.get_company_feed(company)
            feeds[f"{symbol}_stock"] = cls.get_stock_feed(symbol)

        return feeds

class NewsArticle:
    def __init__(self, entry: Dict, category: str):
        self.entry = entry
        self.category = category

    def to_dict(self) -> Dict:
        """Convert feed entry to standardized article format"""
        try:
            # Get published date
            if hasattr(self.entry, 'published_parsed') and self.entry.published_parsed:
                published_at = datetime.fromtimestamp(mktime(self.entry.published_parsed), tz=timezone.utc)
            else:
                published_at = datetime.now(timezone.utc)

            # Generate unique ID
            article_id = hashlib.md5(
                self.entry.get('link', '').encode('utf-8')
            ).hexdigest()

            # Extract source
            source = self._extract_source(self.entry.get('link', ''))

            # Extract stock symbols mentioned in title and summary
            symbols = self._extract_symbols(
                f"{self.entry.get('title', '')} {self.entry.get('summary', '')}"
            )

            return {
                'article_id': article_id,
                'title': self.entry.get('title', ''),
                'url': self.entry.get('link', ''),
                'summary': self.entry.get('summary', ''),
                'published_at': published_at,
                'category': self.category,
                'source': source,
                'symbols': list(symbols),
                'fetch_time': datetime.now(timezone.utc)
            }
        except Exception as e:
            logger.error(f"Error converting article to dict: {e}")
            raise

    def _extract_source(self, url: str) -> str:
        """Extract source from article URL"""
        try:
            domain = urlparse(url).netloc
            return domain.replace('www.', '')
        except:
            return 'unknown'

    def _extract_symbols(self, text: str) -> set:
        """Extract stock symbols from text"""
        symbols = set()
        # Add mentioned company symbols
        for symbol in NewsFeeds.COMPANIES.keys():
            if symbol in text or NewsFeeds.COMPANIES[symbol] in text:
                symbols.add(symbol)
        return symbols

class NewsScraper:
    def __init__(self):
        # MongoDB configuration
        self.mongo_config = {
            'host': 'localhost',
            'port': 27017,
            'username': 'mongo',
            'password': 'mongo',
            'database': 'news_data',
            'auth_source': 'admin'
        }

        # Initialize feeds and session
        self.feeds = NewsFeeds.get_all_feeds()
        self.session: Optional[aiohttp.ClientSession] = None
        self.backoff = BackoffConfig()
        self.running = True

        # Build MongoDB connection URI
        self.mongo_uri = (
            f"mongodb://{self.mongo_config['username']}:{self.mongo_config['password']}"
            f"@{self.mongo_config['host']}:{self.mongo_config['port']}"
            f"/{self.mongo_config['database']}"
            f"?authSource={self.mongo_config['auth_source']}"
        )

        # Initialize MongoDB client
        self.client = motor.motor_asyncio.AsyncIOMotorClient(self.mongo_uri)
        self.db = self.client[self.mongo_config['database']]

        # Configuration
        self.poll_interval = 300  # 5 minutes
        self.batch_size = 3
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
        ]

    async def ensure_session(self):
        """Ensure aiohttp session exists"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                headers={'User-Agent': random.choice(self.user_agents)}
            )

    async def init_database(self):
        """Initialize MongoDB collections and indexes"""
        try:
            # Create main articles collection and indexes
            await self.db.articles.create_index([("article_id", 1)], unique=True)
            await self.db.articles.create_index([("published_at", -1)])
            await self.db.articles.create_index([("category", 1)])
            await self.db.articles.create_index([("source", 1)])
            await self.db.articles.create_index([("symbols", 1)])

            # Create text index for search
            await self.db.articles.create_index([
                ("title", "text"),
                ("summary", "text")
            ])

            logger.info("Database initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    async def fetch_feed(self, category: str, url: str) -> Optional[List[Dict]]:
        """Fetch and parse RSS feed"""
        await self.ensure_session()
        attempts = 0

        while attempts < self.backoff.max_attempts:
            try:
                async with self.session.get(url, timeout=30) as response:
                    if response.status == 200:
                        content = await response.text()
                        feed = feedparser.parse(content)

                        articles = []
                        for entry in feed.entries:
                            try:
                                article = NewsArticle(entry, category).to_dict()
                                articles.append(article)
                            except Exception as e:
                                logger.error(f"Error processing feed entry: {e}")
                                continue

                        return articles

                    elif response.status == 429:  # Too Many Requests
                        delay = self.backoff.calculate_delay(attempts)
                        logger.warning(f"Rate limited for {category}. Waiting {delay:.2f}s")
                        await asyncio.sleep(delay)

                    else:
                        delay = self.backoff.calculate_delay(attempts)
                        logger.error(f"HTTP {response.status} for {category}. Waiting {delay:.2f}s")
                        await asyncio.sleep(delay)

            except Exception as e:
                delay = self.backoff.calculate_delay(attempts)
                logger.error(f"Error fetching {category} feed: {e}")
                await asyncio.sleep(delay)

            attempts += 1

        return None

    async def save_articles(self, articles: List[Dict]):
        """Save articles to MongoDB"""
        if not articles:
            return

        try:
            # Use unordered bulk write to continue on duplicate key errors
            ops = [
                UpdateOne(
                    {'article_id': article['article_id']},
                    {'$set': article},
                    upsert=True
                )
                for article in articles
            ]

            result = await self.db.articles.bulk_write(ops, ordered=False)
            logger.info(f"Saved {result.upserted_count} new articles, "
                       f"modified {result.modified_count} existing articles")

        except Exception as e:
            logger.error(f"Error saving articles: {e}")

    async def process_feed(self, category: str, url: str):
        """Process a single feed"""
        try:
            articles = await self.fetch_feed(category, url)
            if articles:
                await self.save_articles(articles)
                logger.info(f"Processed {len(articles)} articles for {category}")
        except Exception as e:
            logger.error(f"Error processing feed {category}: {e}")

    async def collect_news(self):
        """Collect news from all feeds"""
        while self.running:
            try:
                # Process feeds in batches
                feeds = list(self.feeds.items())
                for i in range(0, len(feeds), self.batch_size):
                    batch = feeds[i:i + self.batch_size]
                    tasks = [
                        self.process_feed(category, url)
                        for category, url in batch
                    ]
                    await asyncio.gather(*tasks)

                    # Add delay between batches
                    if i + self.batch_size < len(feeds):
                        await asyncio.sleep(2)

            except Exception as e:
                logger.error(f"Error in collect_news: {e}")

            await asyncio.sleep(self.poll_interval)

    async def cleanup_old_articles(self):
        """Remove articles older than 30 days"""
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=30)
            result = await self.db.articles.delete_many({
                'published_at': {'$lt': cutoff_date}
            })
            logger.info(f"Removed {result.deleted_count} old articles")
        except Exception as e:
            logger.error(f"Error cleaning up old articles: {e}")

    async def maintenance_task(self):
        """Run periodic maintenance tasks"""
        while self.running:
            await self.cleanup_old_articles()
            await asyncio.sleep(86400)  # Run daily

    async def close(self):
        """Cleanup resources"""
        if self.session and not self.session.closed:
            await self.session.close()
        if self.client:
            self.client.close()

    async def run(self):
        """Run the news scraper"""
        try:
            await self.init_database()
            await asyncio.gather(
                self.collect_news(),
                self.maintenance_task()
            )
        finally:
            await self.close()

def main():
    scraper = NewsScraper()

    def shutdown(signum, frame):
        logger.info("Shutting down...")
        scraper.running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    asyncio.run(scraper.run())

if __name__ == "__main__":
    main()
