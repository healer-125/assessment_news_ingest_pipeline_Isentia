import logging
import sys
from typing import Optional
from datetime import datetime

from src.config import Config
from src.newsapi_client import NewsAPIClient
from src.data_processor import DataProcessor
from src.kinesis_writer import KinesisWriter
from src.scheduler import Scheduler


def setup_logging():
    """Configure logging."""
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def ingest_news_cycle(
    news_client: NewsAPIClient,
    processor: DataProcessor,
    kinesis_writer: KinesisWriter,
    query: str,
    hours_back: int = 24
):
    """
    Execute one cycle of news ingestion.
    
    Args:
        news_client: NewsAPI client
        processor: Data processor
        kinesis_writer: Kinesis writer
        query: Search query
        hours_back: Hours to look back for articles
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=" * 60)
        logger.info("Starting news ingestion cycle")
        logger.info("=" * 60)
        
        # Fetch articles from NewsAPI
        logger.info(f"Fetching articles for query: '{query}'")
        articles = news_client.fetch_all_articles(
            query=query,
            hours_back=hours_back
        )
        
        if not articles:
            logger.warning("No articles fetched from NewsAPI")
            return
        
        # Process articles
        logger.info("Processing articles...")
        processed_articles = processor.process_articles(articles)
        
        if not processed_articles:
            logger.warning("No valid articles after processing")
            return
        
        # Write to Kinesis
        logger.info(f"Writing {len(processed_articles)} articles to Kinesis...")
        success_count, failed_count = kinesis_writer.write_articles(processed_articles)
        
        logger.info("=" * 60)
        logger.info(f"Ingestion cycle completed:")
        logger.info(f"  - Fetched: {len(articles)} articles")
        logger.info(f"  - Processed: {len(processed_articles)} articles")
        logger.info(f"  - Successfully sent to Kinesis: {success_count}")
        logger.info(f"  - Failed: {failed_count}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error in ingestion cycle: {str(e)}", exc_info=True)
        raise


def main():
    """Main entry point."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Validate configuration
        logger.info("Validating configuration...")
        Config.validate()
        logger.info("Configuration validated successfully")
        
        # Initialize components
        logger.info("Initializing components...")
        news_client = NewsAPIClient(api_key=Config.NEWSAPI_KEY)
        processor = DataProcessor()
        kinesis_writer = KinesisWriter(
            stream_name=Config.KINESIS_STREAM_NAME,
            region=Config.AWS_REGION
        )
        
        # Test Kinesis connection
        logger.info("Testing Kinesis connection...")
        if not kinesis_writer.test_connection():
            logger.error("Failed to connect to Kinesis stream. Exiting.")
            sys.exit(1)
        
        # Define ingestion task
        def ingestion_task():
            ingest_news_cycle(
                news_client=news_client,
                processor=processor,
                kinesis_writer=kinesis_writer,
                query=Config.NEWSAPI_QUERY,
                hours_back=24
            )
        
        # Run scheduler
        scheduler = Scheduler(interval_seconds=Config.POLL_INTERVAL_SECONDS)
        
        logger.info("Starting news ingestion pipeline...")
        logger.info(f"Poll interval: {Config.POLL_INTERVAL_SECONDS} seconds")
        logger.info(f"Query: {Config.NEWSAPI_QUERY}")
        logger.info(f"Kinesis stream: {Config.KINESIS_STREAM_NAME}")
        
        scheduler.run_periodic(ingestion_task)
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
