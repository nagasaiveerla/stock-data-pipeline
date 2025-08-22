"""
Data processing module for the stock data pipeline.
Handles data transformation, validation, and orchestration.
"""

import logging
import time
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from .models import PipelineStatus, StockDataBatch, DatabaseOperationResult
from .api_client import api_client
from .database import db_manager
from .config import config

logger = logging.getLogger(__name__)


class DataProcessor:
    """Main data processing orchestrator."""
    
    def __init__(self):
        self.pipeline_config = config.pipeline
        self.lock = threading.Lock()
    
    def process_single_symbol(self, symbol: str, data_type: str = 'daily') -> DatabaseOperationResult:
        """
        Process data for a single stock symbol.
        
        Args:
            symbol: Stock symbol to process
            data_type: Type of data to fetch ('daily' or 'intraday')
        
        Returns:
            DatabaseOperationResult with processing status
        """
        try:
            logger.info(f"Processing {symbol} - {data_type} data")
            
            # Fetch data from API
            if data_type == 'daily':
                api_response = api_client.fetch_daily_data(symbol)
            elif data_type == 'intraday':
                api_response = api_client.fetch_intraday_data(symbol)
            else:
                return DatabaseOperationResult(
                    success=False,
                    error_message=f"Unsupported data type: {data_type}",
                    symbol=symbol
                )
            
            if not api_response.success:
                logger.error(f"Failed to fetch data for {symbol}: {api_response.error_message}")
                return DatabaseOperationResult(
                    success=False,
                    error_message=api_response.error_message,
                    symbol=symbol
                )
            
            # Create stock data batch
            stock_batch = api_client.create_stock_batch(api_response, data_type)
            if not stock_batch:
                return DatabaseOperationResult(
                    success=False,
                    error_message="Failed to create stock batch from API response",
                    symbol=symbol
                )
            
            # Validate and filter data
            validated_batch = self._validate_and_filter_batch(stock_batch)
            if not validated_batch.data_points:
                return DatabaseOperationResult(
                    success=False,
                    error_message="No valid data points after validation",
                    symbol=symbol
                )
            
            # Store data in database
            db_result = db_manager.upsert_stock_data(validated_batch)
            
            if db_result.success:
                logger.info(f"Successfully processed {symbol}: {db_result.records_processed} records")
            else:
                logger.error(f"Database operation failed for {symbol}: {db_result.error_message}")
            
            return db_result
            
        except Exception as e:
            logger.error(f"Unexpected error processing {symbol}: {e}")
            return DatabaseOperationResult(
                success=False,
                error_message=f"Unexpected error: {str(e)}",
                symbol=symbol
            )
    
    def _validate_and_filter_batch(self, batch: StockDataBatch) -> StockDataBatch:
        """
        Validate and filter data points in a batch.
        
        Args:
            batch: Original stock data batch
            
        Returns:
            Filtered batch with valid data points only
        """
        valid_points = []
        
        for point in batch.data_points:
            try:
                # Skip data points with all None/zero prices
                if all(price is None or price == 0 for price in [
                    point.open_price, point.high_price, point.low_price, point.close_price
                ]):
                    logger.debug(f"Skipping data point with no valid prices: {point.symbol} {point.timestamp}")
                    continue
                
                # Skip data points with invalid volume
                if point.volume is not None and point.volume < 0:
                    logger.debug(f"Skipping data point with invalid volume: {point.symbol} {point.timestamp}")
                    continue
                
                # Skip data points from the future
                if point.timestamp > datetime.now():
                    logger.debug(f"Skipping future data point: {point.symbol} {point.timestamp}")
                    continue
                
                valid_points.append(point)
                
            except Exception as e:
                logger.warning(f"Error validating data point for {batch.symbol}: {e}")
                continue
        
        logger.info(f"Filtered batch for {batch.symbol}: {len(valid_points)}/{len(batch.data_points)} valid points")
        
        return StockDataBatch(
            symbol=batch.symbol,
            data_points=valid_points,
            fetch_timestamp=batch.fetch_timestamp,
            source=batch.source
        )
    
    def process_symbols_batch(self, symbols: List[str], data_type: str = 'daily') -> List[DatabaseOperationResult]:
        """
        Process multiple symbols concurrently.
        
        Args:
            symbols: List of stock symbols to process
            data_type: Type of data to fetch ('daily' or 'intraday')
            
        Returns:
            List of DatabaseOperationResult for each symbol
        """
        results = []
        
        with ThreadPoolExecutor(max_workers=self.pipeline_config.max_workers) as executor:
            # Submit tasks
            future_to_symbol = {
                executor.submit(self.process_single_symbol, symbol, data_type): symbol
                for symbol in symbols
            }
            
            # Collect results
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Thread execution failed for {symbol}: {e}")
                    results.append(DatabaseOperationResult(
                        success=False,
                        error_message=f"Thread execution failed: {str(e)}",
                        symbol=symbol
                    ))
        
        return results
    
    def run_full_pipeline(self, data_type: str = 'daily', symbols: Optional[List[str]] = None) -> PipelineStatus:
        """
        Run the complete data pipeline for all configured symbols.
        
        Args:
            data_type: Type of data to fetch ('daily' or 'intraday')
            symbols: Optional list of symbols to process (uses config if None)
            
        Returns:
            PipelineStatus with execution summary
        """
        pipeline_id = str(uuid.uuid4())
        start_time = datetime.now()
        
        # Use provided symbols or default from config
        symbols_to_process = symbols or self.pipeline_config.symbols
        
        pipeline_status = PipelineStatus(
            pipeline_run_id=pipeline_id,
            start_time=start_time,
            symbols_processed=symbols_to_process.copy()
        )
        
        logger.info(f"Starting pipeline run {pipeline_id} for {len(symbols_to_process)} symbols")
        
        try:
            # Test database connection first
            if not db_manager.test_connection():
                pipeline_status.errors.append("Database connection failed")
                pipeline_status.end_time = datetime.now()
                return pipeline_status
            
            # Test API connection
            if not api_client.health_check():
                pipeline_status.errors.append("API health check failed")
                # Continue anyway as it might be temporary
            
            # Process symbols in batches
            batch_size = self.pipeline_config.batch_size
            all_results = []
            
            for i in range(0, len(symbols_to_process), batch_size):
                batch_symbols = symbols_to_process[i:i + batch_size]
                logger.info(f"Processing batch {i//batch_size + 1}: {batch_symbols}")
                
                batch_results = self.process_symbols_batch(batch_symbols, data_type)
                all_results.extend(batch_results)
                
                # Small delay between batches to be API-friendly
                if i + batch_size < len(symbols_to_process):
                    time.sleep(2)
            
            # Analyze results
            for result in all_results:
                if result.success:
                    pipeline_status.successful_symbols.append(result.symbol)
                    pipeline_status.total_records_processed += result.records_processed
                else:
                    pipeline_status.failed_symbols.append(result.symbol)
                    pipeline_status.errors.append(f"{result.symbol}: {result.error_message}")
            
            pipeline_status.end_time = datetime.now()
            
            # Log summary
            logger.info(f"Pipeline {pipeline_id} completed:")
            logger.info(f"  - Success rate: {pipeline_status.success_rate:.1f}%")
            logger.info(f"  - Total records processed: {pipeline_status.total_records_processed}")
            logger.info(f"  - Duration: {pipeline_status.duration_seconds:.2f} seconds")
            logger.info(f"  - Successful symbols: {len(pipeline_status.successful_symbols)}")
            logger.info(f"  - Failed symbols: {len(pipeline_status.failed_symbols)}")
            
            if pipeline_status.failed_symbols:
                logger.warning(f"Failed symbols: {', '.join(pipeline_status.failed_symbols)}")
            
            return pipeline_status
            
        except Exception as e:
            logger.error(f"Pipeline {pipeline_id} failed with error: {e}")
            pipeline_status.errors.append(f"Pipeline failed: {str(e)}")
            pipeline_status.end_time = datetime.now()
            return pipeline_status
    
    def get_pipeline_statistics(self) -> Dict[str, Any]:
        """Get statistics about the current data in the pipeline."""
        try:
            # Get database statistics
            total_records = db_manager.get_stock_data_count()
            symbols_summary = db_manager.get_symbols_summary()
            
            stats = {
                'total_records': total_records,
                'unique_symbols': len(symbols_summary),
                'symbols_summary': symbols_summary,
                'configured_symbols': self.pipeline_config.symbols,
                'last_updated': datetime.now().isoformat()
            }
            
            # Calculate coverage
            configured_symbols = set(self.pipeline_config.symbols)
            symbols_with_data = set(symbols_summary.keys())
            
            stats['coverage'] = {
                'symbols_with_data': len(symbols_with_data),
                'symbols_configured': len(configured_symbols),
                'missing_symbols': list(configured_symbols - symbols_with_data),
                'extra_symbols': list(symbols_with_data - configured_symbols)
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get pipeline statistics: {e}")
            return {
                'error': str(e),
                'last_updated': datetime.now().isoformat()
            }
    
    def cleanup_old_data(self, days_to_keep: int = 365) -> int:
        """Clean up old data from the database."""
        try:
            deleted_count = db_manager.cleanup_old_data(days_to_keep)
            logger.info(f"Cleaned up {deleted_count} old records (kept {days_to_keep} days)")
            return deleted_count
        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")
            return 0


# Global data processor instance
data_processor = DataProcessor()