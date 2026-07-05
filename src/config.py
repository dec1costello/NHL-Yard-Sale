"""Configuration management for NHL data pipeline."""

import datetime
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class PipelineConfig:
    """Pipeline configuration settings."""
    
    # Paths - all relative to project root
    project_root: Path = field(default_factory=lambda: Path(__file__).parent.parent)
    
    @property
    def warehouse_path(self) -> Path:
        return self.project_root / "warehouse" / "duckdb.db"
    
    @property
    def state_path(self) -> Path:
        return self.project_root / "warehouse" / "state"
    
    @property
    def bronze_path(self) -> Path:
        return self.project_root / "data" / "bronze" / "rosters"
    
    @property
    def silver_path(self) -> Path:
        return self.project_root / "data" / "silver"
    
    @property
    def logs_path(self) -> Path:
        return self.project_root / "data" / "logs"
    
    @property
    def teams_file(self) -> Path:
        return self.project_root / "config" / "teams.txt"
    
    # Scraping settings (keep for backward compatibility)
    base_url: str = "https://www.hockey-reference.com/teams/"
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
    request_timeout: int = 30
    rate_limit_seconds: float = 1.2
    max_retries: int = 3
    
    # API Settings
    api_base_url: str = "https://api-web.nhle.com/v1"
    
    @property
    def current_season(self) -> str:
        """
        Current NHL season in display format (e.g., '2025').
        Kept for backward compatibility with existing dbt models.
        """
        return str(self._get_season_year())
    
    @property
    def current_season_api(self) -> str:
        """
        Current NHL season in API format (e.g., '20252026').
        Used for NHL API requests.
        """
        year = self._get_season_year()
        return f"{year}{year + 1}"
    
    def _get_season_year(self) -> int:
        """
        Determine the current NHL season year.
        NHL seasons start in October and end in June.
        If current month is before July, we're in the previous season.
        """
        now = datetime.datetime.now()
        year = now.year
        
        # If before July, we're in the previous season
        if now.month < 10:
            year -= 1
        
        return year
    
    def season_to_api(self, season: str) -> str:
        """
        Convert display season to API format.
        
        Args:
            season: Display season (e.g., '2025')
            
        Returns:
            API season (e.g., '20252026')
        """
        year = int(season)
        return f"{year}{year + 1}"
    
    def api_to_season(self, api_season: str) -> str:
        """
        Convert API season to display format.
        
        Args:
            api_season: API season (e.g., '20252026')
            
        Returns:
            Display season (e.g., '2025')
        """
        return api_season[:4]
    
    def __post_init__(self):
        """Create necessary directories."""
        for path in [self.bronze_path, self.silver_path, self.logs_path, self.state_path]:
            path.mkdir(parents=True, exist_ok=True)


# Singleton config
_config: Optional[PipelineConfig] = None


def get_config() -> PipelineConfig:
    """Get or create pipeline configuration singleton."""
    global _config
    if _config is None:
        _config = PipelineConfig()
    return _config