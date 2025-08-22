"""
Database management module for the stock data pipeline.
Handles PostgreSQL connections and data operations.
"""

import logging
import time
from contextlib import contextmanager
from typing import List, Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql
import psycopg2.pool
from datetime import datetime

from .models import StockDataPoint, DatabaseOperationResult, StockDataBatch
from .config import config

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and operations for stock data."""
    
    def __init__(self):
        self.db_config = config.database
        self._connection_pool = None
        self._initialize_connection_pool()
    
    def _initialize_connection_pool(self) -> None:
        """Initialize connection pool for database operations."""
        try:
            self._connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                host=self.db_config.host,
                port=self.db_config.port,
                database=self.db_config.database,
                user=self.db_config.user,
                password=self.db_config.password,
                cursor_factory=RealDictCursor
            )
            logger.info("Database connection pool initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database connection pool: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        connection = None
        try:
            connection = self._connection_pool.getconn()
            yield connection
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection:
                self._connection_pool.putconn(connection)
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    logger.info("Database connection test successful")
                    return bool(result)
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def create_tables(self) -> bool:
        """Create necessary tables if they don't exist."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS stock_data (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            open_price DECIMAL(10, 4),
            high_price DECIMAL(10, 4),
            low_price DECIMAL(10, 4),
            close_price DECIMAL(10, 4),
            volume BIGINT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp)
        );
        
        CREATE INDEX IF NOT EXISTS idx_stock_data_symbol ON stock_data(symbol);
        CREATE INDEX IF NOT EXISTS idx_stock_data_timestamp ON stock_data(timestamp);
        CREATE INDEX IF NOT EXISTS idx_stock_data_created_at ON stock_data(created_at);
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_table_query)
                    conn.commit()
                    logger.info("Tables created/verified successfully")
                    return True
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            return False
    
    def upsert_stock_data(self, data_batch: StockDataBatch) -> DatabaseOperationResult:
        """
        Insert or update stock data using batch upsert operation.
        Uses PostgreSQL's ON CONFLICT to handle duplicates.
        """
        start_time = time.time()
        
        upsert_query = """
        INSERT INTO stock_data (symbol, timestamp, open_price, high_price, low_price, close_price, volume)
        VALUES %s
        ON CONFLICT (symbol, timestamp)
        DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            updated_at = CURRENT_TIMESTAMP
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Prepare data tuples
                    data_tuples = []
                    for point in data_batch.data_points:
                        data_tuples.append((
                            point.symbol,
                            point.timestamp,
                            float(point.open_price) if point.open_price else None,
                            float(point.high_price) if point.high_price else None,
                            float(point.low_price) if point.low_price else None,
                            float(point.close_price) if point.close_price else None,
                            point.volume
                        ))
                    
                    # Execute batch upsert
                    from psycopg2.extras import execute_values
                    result = execute_values(
                        cursor,
                        upsert_query,
                        data_tuples,
                        template=None,
                        page_size=100,
                        fetch=False
                    )
                    
                    conn.commit()
                    records_processed = len(data_tuples)
                    execution_time = time.time() - start_time
                    
                    logger.info(f"Successfully upserted {records_processed} records for {data_batch.symbol}")
                    
                    return DatabaseOperationResult(
                        success=True,
                        records_processed=records_processed,
                        records_inserted=records_processed,  # Simplified - actual split would need more complex query
                        records_updated=0,
                        symbol=data_batch.symbol,
                        execution_time=execution_time
                    )
                    
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Failed to upsert data for {data_batch.symbol}: {e}")
            
            return DatabaseOperationResult(
                success=False,
                records_processed=0,
                records_inserted=0,
                records_updated=0,
                error_message=str(e),
                symbol=data_batch.symbol,
                execution_time=execution_time
            )
    
    def get_latest_timestamp(self, symbol: str) -> Optional[datetime]:
        """Get the latest timestamp for a given symbol."""
        query = """
        SELECT MAX(timestamp) as latest_timestamp
        FROM stock_data
        WHERE symbol = %s
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (symbol,))
                    result = cursor.fetchone()
                    
                    if result and result['latest_timestamp']:
                        return result['latest_timestamp']
                    return None
                    
        except Exception as e:
            logger.error(f"Failed to get latest timestamp for {symbol}: {e}")
            return None
    
    def get_stock_data_count(self, symbol: Optional[str] = None) -> int:
        """Get count of stock data records."""
        if symbol:
            query = "SELECT COUNT(*) FROM stock_data WHERE symbol = %s"
            params = (symbol,)
        else:
            query = "SELECT COUNT(*) FROM stock_data"
            params = ()
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    result = cursor.fetchone()
                    return result[0] if result else 0
                    
        except Exception as e:
            logger.error(f"Failed to get stock data count: {e}")
            return 0
    
    def get_symbols_summary(self) -> Dict[str, Any]:
        """Get summary statistics for all symbols."""
        query = """
        SELECT 
            symbol,
            COUNT(*) as record_count,
            MIN(timestamp) as earliest_date,
            MAX(timestamp) as latest_date,
            AVG(close_price) as avg_close_price
        FROM stock_data
        GROUP BY symbol
        ORDER BY symbol
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    results = cursor.fetchall()
                    
                    summary = {}
                    for row in results:
                        summary[row['symbol']] = {
                            'record_count': row['record_count'],
                            'earliest_date': row['earliest_date'],
                            'latest_date': row['latest_date'],
                            'avg_close_price': float(row['avg_close_price']) if row['avg_close_price'] else None
                        }
                    
                    return summary
                    
        except Exception as e:
            logger.error(f"Failed to get symbols summary: {e}")
            return {}
    
    def cleanup_old_data(self, days_to_keep: int = 365) -> int:
        """Remove data older than specified days."""
        query = """
        DELETE FROM stock_data
        WHERE created_at < NOW() - INTERVAL '%s days'
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (days_to_keep,))
                    deleted_count = cursor.rowcount
                    conn.commit()
                    
                    logger.info(f"Cleaned up {deleted_count} old records")
                    return deleted_count
                    
        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")
            return 0
    
    def close_connections(self) -> None:
        """Close all connections in the pool."""
        if self._connection_pool:
            self._connection_pool.closeall()
            logger.info("Database connection pool closed")


# Global database manager instance
db_manager = DatabaseManager()