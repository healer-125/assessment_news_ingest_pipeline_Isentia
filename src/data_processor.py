import logging
import hashlib
import json
from typing import Dict, List, Optional
from datetime import datetime
import re

logger = logging.getLogger(__name__)


class DataProcessor:
    """Process and transform NewsAPI articles into structured format."""
    
    @staticmethod
    def generate_article_id(url: str, title: str) -> str:
        """
        Generate a unique article ID from URL and title.
        
        Args:
            url: Article URL
            title: Article title
            
        Returns:
            Unique article ID (SHA256 hash)
        """
        combined = f"{url}:{title}"
        return hashlib.sha256(combined.encode()).hexdigest()
    
    @staticmethod
    def clean_text(text: Optional[str]) -> str:
        """
        Clean and normalize text content.
        
        Args:
            text: Raw text
            
        Returns:
            Cleaned text
        """
        if not text:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        # Remove leading/trailing whitespace
        text = text.strip()
        return text
    
    @staticmethod
    def parse_datetime(date_string: Optional[str]) -> Optional[str]:
        """
        Parse and format datetime string.
        
        Args:
            date_string: ISO format datetime string
            
        Returns:
            Formatted datetime string or None
        """
        if not date_string:
            return None
        
        try:
            # NewsAPI returns ISO 8601 format
            dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            return dt.isoformat()
        except (ValueError, AttributeError) as e:
            logger.warning(f"Error parsing datetime '{date_string}': {str(e)}")
            return None
    
    @staticmethod
    def extract_content(article: Dict) -> str:
        """
        Extract content from article, preferring 'content' over 'description'.
        
        Args:
            article: Article dictionary from NewsAPI
            
        Returns:
            Article content
        """
        content = article.get("content", "")
        if not content or content == "[Removed]":
            content = article.get("description", "")
        return content
    
    @classmethod
    def process_article(cls, article: Dict) -> Optional[Dict]:
        """
        Process a single article from NewsAPI into required format.
        
        Args:
            article: Raw article dictionary from NewsAPI
            
        Returns:
            Processed article dictionary or None if invalid
        """
        try:
            url = article.get("url", "")
            title = article.get("title", "")
            
            if not url or not title:
                logger.warning(f"Skipping article with missing URL or title")
                return None
            
            # Extract source name
            source = article.get("source", {})
            source_name = source.get("name", "Unknown")
            
            # Extract and clean content
            content = cls.extract_content(article)
            content = cls.clean_text(content)
            
            # Generate article ID
            article_id = cls.generate_article_id(url, title)
            
            # Parse published date
            published_at = cls.parse_datetime(article.get("publishedAt"))
            
            # Get author
            author = article.get("author", "Unknown")
            author = cls.clean_text(author) if author else "Unknown"
            
            # Create processed article
            processed = {
                "article_id": article_id,
                "source_name": cls.clean_text(source_name),
                "title": cls.clean_text(title),
                "content": content,
                "url": url,
                "author": author,
                "published_at": published_at,
                "ingested_at": datetime.utcnow().isoformat()
            }
            
            # Validate required fields
            if not processed["article_id"] or not processed["title"]:
                logger.warning(f"Skipping article with invalid required fields")
                return None
            
            return processed
            
        except Exception as e:
            logger.error(f"Error processing article: {str(e)}")
            return None
    
    @classmethod
    def process_articles(cls, articles: List[Dict]) -> List[Dict]:
        """
        Process multiple articles.
        
        Args:
            articles: List of raw article dictionaries
            
        Returns:
            List of processed article dictionaries
        """
        processed = []
        for article in articles:
            processed_article = cls.process_article(article)
            if processed_article:
                processed.append(processed_article)
        
        logger.info(f"Processed {len(processed)}/{len(articles)} articles")
        return processed
