"""
API client module for fetching stock market data.
Handles interactions with Alpha Vantage API with robust error handling and retry logic.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json

from .models import StockDataPoint, APIResponse, StockDataBatch
from .config import config

logger = logging.getLogger(__name__)


class APIClient:
    """Client for fetching stock market data from Alpha Vantage API."""
    
    def __init__(self):
        self.api_config = config.api
        self.session = self._create_session()
        self.last_api_call = 0
        self.api_call_interval = 12  # Alpha Vantage free tier: 5 calls per minute
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry strategy."""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.api_config.retry_attempts,
            backoff_factor=self.api_config.retry_delay,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default headers
        session.headers.update({
            'User-Agent': 'Stock-Pipeline/1.0',
            'Accept': 'application/json',
            'Connection': 'keep-alive'
        })
        
        return session
    
    def _rate_limit(self) -> None:
        """Implement rate limiting to respect API limits."""
        current_time = time.time()
        time_since_last_call = current_time - self.last_api_call
        
        if time_since_last_call < self.api_call_interval:
            sleep_time = self.api_call_interval - time_since_last_call
            logger.info(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_api_call = time.time()
    
    def _parse_daily_data(self, data: Dict[str, Any], symbol: str) -> List[StockDataPoint]:
        """Parse daily time series data from Alpha Vantage response."""
        try:
            time_series = data.get('Time Series (Daily)', {})
            data_points = []
            
            for date_str, values in time_series.items():
                try:
                    # Parse timestamp
                    timestamp = datetime.strptime(date_str, '%Y-%m-%d')
                    
                    # Create stock data point
                    data_point = StockDataPoint(
                        symbol=symbol,
                        timestamp=timestamp,
                        open_price=float(values.get('1. open', 0)) if values.get('1. open') else None,
                        high_price=float(values.get('2. high', 0)) if values.get('2. high') else None,
                        low_price=float(values.get('3. low', 0)) if values.get('3. low') else None,
                        close_price=float(values.get('4. close', 0)) if values.get('4. close') else None,
                        volume=int(values.get('5. volume', 0)) if values.get('5. volume') else None
                    )
                    
                    data_points.append(data_point)
                    
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to parse data point for {symbol} on {date_str}: {e}")
                    continue
            
            logger.info(f"Successfully parsed {len(data_points)} data points for {symbol}")
            return data_points
            
        except Exception as e:
            logger.error(f"Failed to parse daily data for {symbol}: {e}")
            return []
    
    def _parse_intraday_data(self, data: Dict[str, Any], symbol: str) -> List[StockDataPoint]:
        """Parse intraday time series data from Alpha Vantage response."""
        try:
            # Find the time series key (it varies based on interval)
            time_series_key = None
            for key in data.keys():
                if key.startswith('Time Series'):
                    time_series_key = key
                    break
            
            if not time_series_key:
                logger.error(f"No time series data found for {symbol}")
                return []
            
            time_series = data.get(time_series_key, {})
            data_points = []
            
            for datetime_str, values in time_series.items():
                try:
                    # Parse timestamp (format: YYYY-MM-DD HH:MM:SS)
                    timestamp = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                    
                    # Create stock data point
                    data_point = StockDataPoint(
                        symbol=symbol,
                        timestamp=timestamp,
                        open_price=float(values.get('1. open', 0)) if values.get('1. open') else None,
                        high_price=float(values.get('2. high', 0)) if values.get('2. high') else None,
                        low_price=float(values.get('3. low', 0)) if values.get('3. low') else None,
                        close_price=float(values.get('4. close', 0)) if values.get('4. close') else None,
                        volume=int(values.get('5. volume', 0)) if values.get('5. volume') else None
                    )
                    
                    data_points.append(data_point)
                    
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to parse intraday data point for {symbol} at {datetime_str}: {e}")
                    continue
            
            logger.info(f"Successfully parsed {len(data_points)} intraday data points for {symbol}")
            return data_points
            
        except Exception as e:
            logger.error(f"Failed to parse intraday data for {symbol}: {e}")
            return []
    
    def fetch_daily_data(self, symbol: str, outputsize: str = 'compact') -> APIResponse:
        """
        Fetch daily stock data for a given symbol.
        
        Args:
            symbol: Stock symbol to fetch
            outputsize: 'compact' (100 data points) or 'full' (20+ years)
        """
        self._rate_limit()
        
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'outputsize': outputsize,
            'apikey': self.api_config.alpha_vantage_key
        }
        
        try:
            logger.info(f"Fetching daily data for {symbol}")
            
            response = self.session.get(
                self.api_config.base_url,
                params=params,
                timeout=self.api_config.timeout
            )
            
            response.raise_for_status()
            data = response.json()
            
            # Check for API errors
            if 'Error Message' in data:
                error_msg = data['Error Message']
                logger.error(f"API error for {symbol}: {error_msg}")
                return APIResponse(
                    success=False,
                    error_message=error_msg,
                    symbol=symbol
                )
            
            if 'Note' in data:
                note_msg = data['Note']
                logger.warning(f"API note for {symbol}: {note_msg}")
                return APIResponse(
                    success=False,
                    error_message=f"API limit reached: {note_msg}",
                    symbol=symbol
                )
            
            # Check if we have valid data
            if 'Time Series (Daily)' not in data:
                logger.error(f"No daily time series data found for {symbol}")
                return APIResponse(
                    success=False,
                    error_message="No time series data in response",
                    symbol=symbol
                )
            
            return APIResponse(
                success=True,
                data=data,
                symbol=symbol
            )
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {symbol}: {e}")
            return APIResponse(
                success=False,
                error_message=f"Request failed: {str(e)}",
                symbol=symbol
            )
        
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response for {symbol}: {e}")
            return APIResponse(
                success=False,
                error_message=f"Invalid JSON response: {str(e)}",
                symbol=symbol
            )
        
        except Exception as e:
            logger.error(f"Unexpected error fetching data for {symbol}: {e}")
            return APIResponse(
                success=False,
                error_message=f"Unexpected error: {str(e)}",
                symbol=symbol
            )
    
    def fetch_intraday_data(self, symbol: str, interval: str = '60min') -> APIResponse:
        """
        Fetch intraday stock data for a given symbol.
        
        Args:
            symbol: Stock symbol to fetch
            interval: '1min', '5min', '15min', '30min', '60min'
        """
        self._rate_limit()
        
        params = {
            'function': 'TIME_SERIES_INTRADAY',
            'symbol': symbol,
            'interval': interval,
            'outputsize': 'compact',
            'apikey': self.api_config.alpha_vantage_key
        }
        
        try:
            logger.info(f"Fetching intraday data for {symbol} with interval {interval}")
            
            response = self.session.get(
                self.api_config.base_url,
                params=params,
                timeout=self.api_config.timeout
            )
            
            response.raise_for_status()
            data = response.json()
            
            # Check for API errors
            if 'Error Message' in data:
                error_msg = data['Error Message']
                logger.error(f"API error for {symbol}: {error_msg}")
                return APIResponse(
                    success=False,
                    error_message=error_msg,
                    symbol=symbol
                )
            
            if 'Note' in data:
                note_msg = data['Note']
                logger.warning(f"API note for {symbol}: {note_msg}")
                return APIResponse(
                    success=False,
                    error_message=f"API limit reached: {note_msg}",
                    symbol=symbol
                )
            
            return APIResponse(
                success=True,
                data=data,
                symbol=symbol
            )
            
        except Exception as e:
            logger.error(f"Failed to fetch intraday data for {symbol}: {e}")
            return APIResponse(
                success=False,
                error_message=str(e),
                symbol=symbol
            )
    
    def create_stock_batch(self, api_response: APIResponse, data_type: str = 'daily') -> Optional[StockDataBatch]:
        """Create a StockDataBatch from API response."""
        if not api_response.success or not api_response.data:
            return None
        
        try:
            if data_type == 'daily':
                data_points = self._parse_daily_data(api_response.data, api_response.symbol)
            elif data_type == 'intraday':
                data_points = self._parse_intraday_data(api_response.data, api_response.symbol)
            else:
                logger.error(f"Unsupported data type: {data_type}")
                return None
            
            if not data_points:
                logger.warning(f"No valid data points parsed for {api_response.symbol}")
                return None
            
            return StockDataBatch(
                symbol=api_response.symbol,
                data_points=data_points,
                fetch_timestamp=datetime.now(),
                source='alpha_vantage'
            )
            
        except Exception as e:
            logger.error(f"Failed to create stock batch for {api_response.symbol}: {e}")
            return None
    
    def health_check(self) -> bool:
        """Perform a health check on the API."""
        try:
            # Use a simple API call to check connectivity
            test_response = self.fetch_daily_data('AAPL', 'compact')
            return test_response.success
            
        except Exception as e:
            logger.error(f"API health check failed: {e}")
            return False


# Global API client instance
api_client = APIClient()