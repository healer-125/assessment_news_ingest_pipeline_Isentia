import os
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Application configuration loaded from environment variables."""
    
    # NewsAPI Configuration
    NEWSAPI_KEY: str = os.getenv("NEWSAPI_KEY", "")
    NEWSAPI_BASE_URL: str = "https://newsapi.org/v2/everything"
    NEWSAPI_QUERY: str = os.getenv("NEWSAPI_QUERY", "technology")
    NEWSAPI_PAGE_SIZE: int = int(os.getenv("NEWSAPI_PAGE_SIZE", "100"))
    NEWSAPI_SORT_BY: str = os.getenv("NEWSAPI_SORT_BY", "publishedAt")
    NEWSAPI_LANGUAGE: str = os.getenv("NEWSAPI_LANGUAGE", "en")
    
    # AWS Kinesis Configuration
    AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
    KINESIS_STREAM_NAME: str = os.getenv("KINESIS_STREAM_NAME", "news-ingest-stream")
    KINESIS_BATCH_SIZE: int = int(os.getenv("KINESIS_BATCH_SIZE", "500"))
    
    # Application Configuration
    POLL_INTERVAL_SECONDS: int = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))  # 5 minutes
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAY_SECONDS: int = int(os.getenv("RETRY_DELAY_SECONDS", "5"))
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    @classmethod
    def validate(cls) -> bool:
        """Validate that required configuration is present."""
        if not cls.NEWSAPI_KEY:
            raise ValueError("NEWSAPI_KEY environment variable is required")
        if not cls.KINESIS_STREAM_NAME:
            raise ValueError("KINESIS_STREAM_NAME environment variable is required")
        return True
