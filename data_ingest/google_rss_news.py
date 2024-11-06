#!/usr/bin/env python3

import asyncio
import aiohttp
import feedparser
import logging
from datetime import datetime, timezone, timedelta
import motor.motor_asyncio
from typing import Dict, List, Optional, Set
import signal
from dataclasses import dataclass
import random
import hashlib
from urllib.parse import quote, urlparse
from time import mktime
from pymongo import UpdateOne, ASCENDING, DESCENDING, TEXT
import re
from bs4 import BeautifulSoup
from googlenewsdecoder import new_decoderv1

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_ingest:google_rss_news')

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

class NewsFeeds:
    """Financial news feed URLs manager"""

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
    """Process and structure Google News RSS entries"""

    def __init__(self, entry: Dict, category: str):
        self.entry = entry
        self.category = category

    def _extract_source(self) -> str:
        """Extract source from Google News entry"""
        try:
            if hasattr(self.entry, 'source'):
                return self.entry.source.get('title', 'unknown')

            # Fallback to source from link
            domain = urlparse(self.entry.get('link', '')).netloc
            return domain.replace('www.', '')
        except Exception as e:
            logger.warning(f"Error extracting source: {e}")
            return 'unknown'

    async def _extract_content(self, url:str) -> str:
        # """Scrapes article content by following redirects and extracting text. Also sets URL"""
        # headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36'}

        async with aiohttp.ClientSession() as session:
            try:
                decoded_url = new_decoderv1(url)["decoded_url"]
                async with session.get(decoded_url) as response: # , headers=headers
                    if response.status != 200:
                        logger.warning(f'HTTP {response.status}: Failed to fetch content')
                    html_content = await response.text()
                    # Parse the HTML content
                    content = BeautifulSoup(html_content, 'html.parser').get_text()
                    return decoded_url, content

            except aiohttp.ClientError as e:
                logger.error(f"Network error while fetching {url}: {str(e)}")
            except Exception as e:
                logger.error(f"Error processing {url}: {str(e)}")

    def _extract_symbols(self, text: str) -> Set[str]:
        """Extract stock symbols from text"""
        symbols = set()
        for symbol in NewsFeeds.COMPANIES.keys():
            if re.search(rf'\b{symbol}\b', text):
                symbols.add(symbol)

        # Add company name matches
        for symbol, company in NewsFeeds.COMPANIES.items():
            if re.search(rf'\b{company}\b', text, re.IGNORECASE):
                symbols.add(symbol)
        return symbols

    def _get_published_date(self) -> datetime:
        """Extract and validate published date"""
        try:
            if hasattr(self.entry, 'published_parsed') and self.entry.published_parsed:
                return datetime.fromtimestamp(mktime(self.entry.published_parsed), tz=timezone.utc)
        except Exception as e:
            logger.warning(f"Error parsing published date: {e}")
        return datetime.now(timezone.utc)

    def _clean_title(self, title: str) -> str:
        """Clean the title, removing source if present"""
        if ' - ' in title:
            return title.split(' - ')[0].strip()
        return title.strip()

    async def to_dict(self) -> Dict:
        """Convert feed entry to standardized article format"""
        try:
            # Get URL and generate ID

            # Clean title and summary
            title = self._clean_title(self.entry.get('title', ''))
            # summary = self._clean_html_summary(self.entry.get('summary', ''))

            # Extract symbols from title
            # TODO: consider adding content to extract symbols
            symbols = self._extract_symbols(title)
            content = await self._extract_content(self.entry.get('link', ''))
            if not content:
                return
            url, content = content
            article_id = hashlib.md5(url.encode('utf-8')).hexdigest()

            return {
                'article_id': article_id,
                'title': title,
                'content': content,
                'url': url,
                'content': content,
                'published_at': self._get_published_date(),
                'category': self.category,
                'source': self._extract_source(),
                'symbols': list(symbols),
                'fetch_time': datetime.now(timezone.utc)
            }
        except Exception as e:
            logger.error(f"Error converting article to dict: {e}")
            raise

class NewsScraper:
    """Google News RSS feed scraper with MongoDB storage"""

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

        # Initialize components
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
        self.client = motor.motor_asyncio.AsyncIOMotorClient(
            self.mongo_uri,
            serverSelectionTimeoutMS=5000
        )
        self.db = self.client[self.mongo_config['database']]

        # Operating parameters
        self.poll_interval = 300  # 5 minutes
        self.batch_size = 3
        self.cleanup_interval = 86400  # 24 hours
        self.retention_days = 30
        self.maintain = False

        # Request headers
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
        ]

    async def ensure_session(self):
        """Ensure aiohttp session exists"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                headers={'User-Agent': random.choice(self.user_agents)},
                timeout=aiohttp.ClientTimeout(total=30)
            )

    async def init_database(self):
        """Initialize MongoDB collections and indexes"""
        try:
            # Create indexes
            await self.db.articles.create_index([("article_id", ASCENDING)], unique=True)
            await self.db.articles.create_index([("published_at", DESCENDING)])
            await self.db.articles.create_index([("category", ASCENDING)])
            await self.db.articles.create_index([("source", ASCENDING)])
            await self.db.articles.create_index([("symbols", ASCENDING)])
            await self.db.articles.create_index([
                ("title", TEXT),
                ("content", TEXT)
            ])

            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    async def fetch_feed(self, category: str, url: str) -> Optional[List[Dict]]:
        """Fetch and parse RSS feed with retries"""
        await self.ensure_session()
        attempts = 0

        while attempts < self.backoff.max_attempts:
            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        content = await response.text()
                        feed = feedparser.parse(content)

                        if hasattr(feed, 'bozo_exception'):
                            logger.warning(f"Feed parsing warning for {category}: {feed.bozo_exception}")

                        articles = []
                        for entry in feed.entries:
                            try:
                                article = await NewsArticle(entry, category).to_dict()
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
        """Save articles to MongoDB using bulk operations"""
        if not articles:
            return

        try:
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
        """Collect news from all feeds with batching"""
        while self.running:
            try:
                feeds = list(self.feeds.items())
                for i in range(0, len(feeds), self.batch_size):
                    batch = feeds[i:i + self.batch_size]
                    tasks = [
                        self.process_feed(category, url)
                        for category, url in batch
                    ]
                    await asyncio.gather(*tasks)

                    if i + self.batch_size < len(feeds):
                        await asyncio.sleep(2)

            except Exception as e:
                logger.error(f"Error in collect_news: {e}")

            await asyncio.sleep(self.poll_interval)

    async def cleanup_old_articles(self):
        """Remove articles older than retention period"""
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=self.retention_days)
            result = await self.db.articles.delete_many({
                'published_at': {'$lt': cutoff_date}
            })
            logger.info(f"Removed {result.deleted_count} old articles")
        except Exception as e:
            logger.error(f"Error cleaning up old articles: {e}")

    async def maintenance_task(self):
        """Run periodic maintenance tasks"""
        while self.running and self.maintain:
            await self.cleanup_old_articles()
            await asyncio.sleep(self.cleanup_interval)

    async def close(self):
        """Cleanup resources"""
        try:
            if self.session and not self.session.closed:
                await self.session.close()
            if self.client:
                self.client.close()
            logger.info("Resources cleaned up successfully")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    async def run(self):
        """Run the news scraper with proper task management"""
        try:
            # Initialize database first
            await self.init_database()

            # Create tasks
            news_task = asyncio.create_task(
                self.collect_news(),
                name="news_collection"
            )
            maintenance_task = asyncio.create_task(
                self.maintenance_task(),
                name="maintenance"
            )

            # Run tasks concurrently
            running_tasks = [news_task, maintenance_task]

            # Wait for tasks to complete or be cancelled
            done, pending = await asyncio.wait(
                running_tasks,
                return_when=asyncio.FIRST_EXCEPTION
            )

            # Check for exceptions
            for task in done:
                if task.exception():
                    logger.error(f"Task failed with error: {task.exception()}")
                    raise task.exception()

        except Exception as e:
            logger.error(f"Error in main run loop: {e}")
            raise
        finally:
            # Ensure cleanup happens
            await self.close()

            # Cancel any pending tasks
            for task in asyncio.all_tasks():
                if task is not asyncio.current_task():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

async def startup_sequence():
    """Startup sequence with health checks"""
    try:
        scraper = NewsScraper()

        # Test database connection
        await scraper.init_database()

        # Test feed fetching
        test_feed = next(iter(scraper.feeds.items()))
        articles = await scraper.fetch_feed(test_feed[0], test_feed[1])
        if not articles:
            raise Exception("Failed to fetch test feed")

        logger.info("Startup checks completed successfully")
        return scraper
    except Exception as e:
        logger.error(f"Startup sequence failed: {e}")
        raise

async def shutdown(loop, signal=None):
    """Cleanup function for graceful shutdown"""
    try:
        if signal:
            logger.info(f"Received exit signal {signal.name}")

        logger.info("Initiating shutdown sequence...")

        # Stop accepting new tasks
        loop.stop()

        # Stop the scraper's main loop
        if scraper:
            scraper.running = False
            await scraper.close()

        # Cancel all running tasks
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if tasks:
            logger.info(f"Cancelling {len(tasks)} outstanding tasks")
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        logger.info("Shutdown completed successfully")

    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
        raise
    finally:
        # Ensure the loop is stopped even if an error occurred
        if not loop.is_closed():
            loop.stop()

def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    scraper = None


    def handle_exception(loop, context):
        """Global exception handler"""
        msg = context.get("exception", context["message"])
        logger.error(f"Caught global exception: {msg}")
        asyncio.create_task(shutdown(loop))

    try:
        # Set up signal handlers
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(shutdown(loop, sig))
            )

        # Set up global exception handler
        loop.set_exception_handler(handle_exception)

        # Start the scraper
        scraper = loop.run_until_complete(startup_sequence())
        loop.run_until_complete(scraper.run())
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
    finally:
        try:
            loop.run_until_complete(shutdown(loop))
            loop.close()
            logger.info("Shutdown complete")
        except Exception as e:
            logger.error(f"Error during final cleanup: {e}")

if __name__ == "__main__":
    main()
