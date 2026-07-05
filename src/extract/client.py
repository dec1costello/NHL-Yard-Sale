"""Generic NHL API client with retries and session management."""

import time
import logging
from typing import Optional, Dict, Any
from datetime import datetime

import requests

from src.utils.logging import get_logger

logger = get_logger(__name__)


class NHLAPIClient:
    """
    Reusable NHL API client.
    
    Base URL: https://api-web.nhle.com/v1
    """
    
    BASE_URL = "https://api-web.nhle.com/v1"
    
    def __init__(self, timeout: int = 30, max_retries: int = 3):
        """
        Initialize the API client.
        
        Args:
            timeout: Request timeout in seconds
            max_retries: Number of retry attempts for failed requests
        """
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        })
    
    def _get(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make a GET request with retries.
        
        Args:
            endpoint: API endpoint (e.g., "roster/TOR/20252026")
            params: Optional query parameters
            
        Returns:
            Parsed JSON response
            
        Raises:
            requests.RequestException: After all retries fail
        """
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"API Request: {url}")
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.timeout
                )
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.Timeout as e:
                logger.warning(f"Timeout on attempt {attempt + 1}: {e}")
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"Connection error on attempt {attempt + 1}: {e}")
            except requests.exceptions.HTTPError as e:
                # Don't retry on 4xx errors
                if response.status_code < 500:
                    logger.error(f"Client error {response.status_code}: {e}")
                    raise
                logger.warning(f"Server error {response.status_code} on attempt {attempt + 1}: {e}")
            
            if attempt < self.max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                logger.info(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise RuntimeError(f"Failed to fetch {url} after {self.max_retries} attempts")
        
        raise RuntimeError(f"Failed to fetch {url} after {self.max_retries} attempts")
    
    # ============================================================
    # Roster Endpoints
    # ============================================================
    
    def get_roster_current(self, team: str) -> Dict[str, Any]:
        """
        Get current roster for a team.
        
        Args:
            team: Three-letter team code (e.g., 'TOR', 'COL')
            
        Returns:
            Raw API response
        """
        return self._get(f"roster/{team}/current")
    
    def get_roster_by_season(self, team: str, season: int) -> Dict[str, Any]:
        """
        Get roster for a team and season.
        
        Args:
            team: Three-letter team code (e.g., 'TOR', 'COL')
            season: Season in YYYYYYYY format (e.g., 20252026)
            
        Returns:
            Raw API response
        """
        return self._get(f"roster/{team}/{season}")
    
    def get_roster_seasons(self, team: str) -> Dict[str, Any]:
        """
        Get all seasons a team played.
        
        Args:
            team: Three-letter team code
            
        Returns:
            Raw API response
        """
        return self._get(f"roster-season/{team}")
    
    def get_prospects(self, team: str) -> Dict[str, Any]:
        """
        Get prospects for a team.
        
        Args:
            team: Three-letter team code
            
        Returns:
            Raw API response
        """
        return self._get(f"prospects/{team}")