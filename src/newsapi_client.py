import logging
import requests
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import time

from src.config import Config

logger = logging.getLogger(__name__)


class NewsAPIClient:
    """Client for interacting with NewsAPI Everything endpoint."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = Config.NEWSAPI_BASE_URL
        self.session = requests.Session()
        self.session.headers.update({
            "X-Api-Key": api_key
        })
    
    def fetch_articles(
        self,
        query: str,
        page_size: int = 100,
        sort_by: str = "publishedAt",
        language: str = "en",
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        page: int = 1
    ) -> Dict:
        """
        Fetch articles from NewsAPI Everything endpoint.
        
        Args:
            query: Search query
            page_size: Number of results per page (max 100)
            sort_by: Sort order (relevancy, popularity, publishedAt)
            language: Language code (e.g., 'en')
            from_date: Start date for articles
            to_date: End date for articles
            page: Page number
            
        Returns:
            Dictionary containing API response with articles
        """
        params = {
            "q": query,
            "pageSize": min(page_size, 100),  # API max is 100
            "sortBy": sort_by,
            "language": language,
            "page": page
        }
        
        if from_date:
            params["from"] = from_date.isoformat()
        if to_date:
            params["to"] = to_date.isoformat()
        
        try:
            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get("status") == "error":
                error_msg = data.get("message", "Unknown error")
                logger.error(f"NewsAPI error: {error_msg}")
                raise Exception(f"NewsAPI error: {error_msg}")
            
            logger.info(
                f"Fetched {len(data.get('articles', []))} articles "
                f"(page {page}, total: {data.get('totalResults', 0)})"
            )
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching articles from NewsAPI: {str(e)}")
            raise
    
    def fetch_all_articles(
        self,
        query: str,
        max_pages: Optional[int] = None,
        hours_back: int = 24
    ) -> List[Dict]:
        """
        Fetch all articles across multiple pages.
        
        Args:
            query: Search query
            max_pages: Maximum number of pages to fetch (None for all)
            hours_back: Number of hours to look back for articles
            
        Returns:
            List of article dictionaries
        """
        all_articles = []
        page = 1
        to_date = datetime.utcnow()
        from_date = to_date - timedelta(hours=hours_back)
        
        while True:
            try:
                response = self.fetch_articles(
                    query=query,
                    page_size=Config.NEWSAPI_PAGE_SIZE,
                    sort_by=Config.NEWSAPI_SORT_BY,
                    language=Config.NEWSAPI_LANGUAGE,
                    from_date=from_date,
                    to_date=to_date,
                    page=page
                )
                
                articles = response.get("articles", [])
                if not articles:
                    break
                
                all_articles.extend(articles)
                
                total_results = response.get("totalResults", 0)
                total_pages = (total_results + Config.NEWSAPI_PAGE_SIZE - 1) // Config.NEWSAPI_PAGE_SIZE
                
                if page >= total_pages:
                    break
                
                if max_pages and page >= max_pages:
                    break
                
                page += 1
                
                # Rate limiting: NewsAPI free tier allows 100 requests/day
                # Be respectful and add a small delay between requests
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error fetching page {page}: {str(e)}")
                break
        
        logger.info(f"Total articles fetched: {len(all_articles)}")
        return all_articles
