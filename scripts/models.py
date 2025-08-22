"""
Data models for the stock data pipeline.
Contains Pydantic models for data validation and type safety.
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, validator
import logging

logger = logging.getLogger(__name__)


class StockDataPoint(BaseModel):
    """Model for individual stock data point."""
    
    symbol: str = Field(..., min_length=1, max_length=10, description="Stock symbol")
    timestamp: datetime = Field(..., description="Data timestamp")
    open_price: Optional[Decimal] = Field(None, ge=0, description="Opening price")
    high_price: Optional[Decimal] = Field(None, ge=0, description="Highest price")
    low_price: Optional[Decimal] = Field(None, ge=0, description="Lowest price") 
    close_price: Optional[Decimal] = Field(None, ge=0, description="Closing price")
    volume: Optional[int] = Field(None, ge=0, description="Trading volume")
    
    class Config:
        """Pydantic configuration."""
        validate_assignment = True
        use_enum_values = True
        
    @validator('symbol')
    def validate_symbol(cls, v):
        """Validate stock symbol format."""
        if not v or not v.strip():
            raise ValueError("Symbol cannot be empty")
        return v.upper().strip()
    
    @validator('timestamp')
    def validate_timestamp(cls, v):
        """Validate timestamp is not in the future."""
        if v > datetime.now():
            logger.warning(f"Timestamp {v} is in the future")
        return v
    
    @validator('high_price')
    def validate_high_price(cls, v, values):
        """Validate high price is >= low price if both exist."""
        if v is not None and 'low_price' in values and values['low_price'] is not None:
            if v < values['low_price']:
                raise ValueError("High price cannot be less than low price")
        return v


class APIResponse(BaseModel):
    """Model for API response wrapper."""
    
    success: bool = Field(..., description="Whether the API call was successful")
    data: Optional[Dict[str, Any]] = Field(None, description="Response data")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    symbol: str = Field(..., description="Stock symbol requested")
    timestamp: datetime = Field(default_factory=datetime.now, description="Response timestamp")


class DatabaseOperationResult(BaseModel):
    """Model for database operation results."""
    
    success: bool = Field(..., description="Whether the operation was successful")
    records_processed: int = Field(0, ge=0, description="Number of records processed")
    records_inserted: int = Field(0, ge=0, description="Number of new records inserted")
    records_updated: int = Field(0, ge=0, description="Number of records updated")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    symbol: str = Field(..., description="Stock symbol processed")
    execution_time: Optional[float] = Field(None, ge=0, description="Execution time in seconds")


class PipelineStatus(BaseModel):
    """Model for overall pipeline status."""
    
    pipeline_run_id: str = Field(..., description="Unique pipeline run identifier")
    start_time: datetime = Field(..., description="Pipeline start time")
    end_time: Optional[datetime] = Field(None, description="Pipeline end time")
    symbols_processed: list[str] = Field(default_factory=list, description="List of symbols processed")
    total_records_processed: int = Field(0, ge=0, description="Total records across all symbols")
    successful_symbols: list[str] = Field(default_factory=list, description="Successfully processed symbols")
    failed_symbols: list[str] = Field(default_factory=list, description="Failed symbols")
    errors: list[str] = Field(default_factory=list, description="List of errors encountered")
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        total = len(self.symbols_processed)
        if total == 0:
            return 0.0
        return (len(self.successful_symbols) / total) * 100
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate pipeline duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


class StockDataBatch(BaseModel):
    """Model for batch of stock data points."""
    
    symbol: str = Field(..., description="Stock symbol for this batch")
    data_points: list[StockDataPoint] = Field(default_factory=list, description="List of data points")
    fetch_timestamp: datetime = Field(default_factory=datetime.now, description="When this batch was fetched")
    source: str = Field(default="alpha_vantage", description="Data source")
    
    @validator('data_points')
    def validate_data_points(cls, v, values):
        """Validate all data points have the same symbol."""
        if 'symbol' in values:
            expected_symbol = values['symbol']
            for point in v:
                if point.symbol != expected_symbol:
                    raise ValueError(f"Data point symbol {point.symbol} doesn't match batch symbol {expected_symbol}")
        return v
    
    @property
    def record_count(self) -> int:
        """Get number of records in this batch."""
        return len(self.data_points)
    
    @property  
    def date_range(self) -> Optional[tuple[datetime, datetime]]:
        """Get date range of data points in this batch."""
        if not self.data_points:
            return None
        
        timestamps = [point.timestamp for point in self.data_points]
        return min(timestamps), max(timestamps)