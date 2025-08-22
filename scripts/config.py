"""
Configuration management module for the stock data pipeline.
Handles environment variables and configuration settings.
"""

import os
from typing import Dict, List
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    host: str
    port: int
    database: str
    user: str
    password: str
    
    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class APIConfig:
    """API configuration settings."""
    alpha_vantage_key: str
    base_url: str = "https://www.alphavantage.co/query"
    timeout: int = 30
    retry_attempts: int = 3
    retry_delay: int = 5


@dataclass
class PipelineConfig:
    """Pipeline configuration settings."""
    symbols: List[str]
    batch_size: int = 5
    max_workers: int = 3
    
    
class ConfigManager:
    """Centralized configuration manager."""
    
    def __init__(self):
        self._validate_environment()
        
    def _validate_environment(self) -> None:
        """Validate that all required environment variables are set."""
        required_vars = [
            'POSTGRES_HOST',
            'POSTGRES_DB', 
            'POSTGRES_USER',
            'POSTGRES_PASSWORD',
            'ALPHA_VANTAGE_API_KEY'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    @property
    def database(self) -> DatabaseConfig:
        """Get database configuration."""
        return DatabaseConfig(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=int(os.getenv('POSTGRES_PORT', '5432')),
            database=os.getenv('POSTGRES_DB', 'stockdata'),
            user=os.getenv('POSTGRES_USER', 'stockuser'),
            password=os.getenv('POSTGRES_PASSWORD', 'stockpass')
        )
    
    @property
    def api(self) -> APIConfig:
        """Get API configuration."""
        return APIConfig(
            alpha_vantage_key=os.getenv('ALPHA_VANTAGE_API_KEY'),
            timeout=int(os.getenv('API_TIMEOUT', '30')),
            retry_attempts=int(os.getenv('API_RETRY_ATTEMPTS', '3')),
            retry_delay=int(os.getenv('API_RETRY_DELAY', '5'))
        )
    
    @property
    def pipeline(self) -> PipelineConfig:
        """Get pipeline configuration."""
        symbols_str = os.getenv('STOCK_SYMBOLS', 'AAPL,GOOGL,MSFT,TSLA,AMZN')
        symbols = [s.strip().upper() for s in symbols_str.split(',')]
        
        return PipelineConfig(
            symbols=symbols,
            batch_size=int(os.getenv('BATCH_SIZE', '5')),
            max_workers=int(os.getenv('MAX_WORKERS', '3'))
        )


# Global configuration instance
config = ConfigManager()