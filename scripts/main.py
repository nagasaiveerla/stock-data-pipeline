"""
Main script for running the stock data pipeline independently.
Can be used for testing or running the pipeline outside of Airflow.
"""

import argparse
import logging
import sys
from datetime import datetime
from typing import List, Optional

from config import config
from data_processor import data_processor
from database import db_manager
from api_client import api_client


def setup_logging(log_level: str = 'INFO') -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Reduce noise from third-party libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)


def test_connections() -> bool:
    """Test all system connections."""
    logger = logging.getLogger(__name__)
    
    logger.info("Testing system connections...")
    
    # Test database connection
    db_ok = db_manager.test_connection()
    if db_ok:
        logger.info("✓ Database connection successful")
        # Ensure tables exist
        db_manager.create_tables()
    else:
        logger.error("✗ Database connection failed")
    
    # Test API connection
    api_ok = api_client.health_check()
    if api_ok:
        logger.info("✓ API connection successful")
    else:
        logger.error("✗ API connection failed")
    
    return db_ok and api_ok


def run_pipeline(data_type: str = 'daily', symbols: Optional[List[str]] = None) -> bool:
    """Run the complete pipeline."""
    logger = logging.getLogger(__name__)
    
    logger.info(f"Starting {data_type} pipeline...")
    
    # Test connections first
    if not test_connections():
        logger.error("Connection tests failed - aborting pipeline")
        return False
    
    # Run pipeline
    pipeline_status = data_processor.run_full_pipeline(data_type, symbols)
    
    # Print summary
    logger.info("=" * 60)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Pipeline ID: {pipeline_status.pipeline_run_id}")
    logger.info(f"Data Type: {data_type}")
    logger.info(f"Symbols Processed: {len(pipeline_status.symbols_processed)}")
    logger.info(f"Successful: {len(pipeline_status.successful_symbols)}")
    logger.info(f"Failed: {len(pipeline_status.failed_symbols)}")
    logger.info(f"Success Rate: {pipeline_status.success_rate:.1f}%")
    logger.info(f"Total Records: {pipeline_status.total_records_processed}")
    logger.info(f"Duration: {pipeline_status.duration_seconds:.2f} seconds")
    
    if pipeline_status.failed_symbols:
        logger.warning(f"Failed Symbols: {', '.join(pipeline_status.failed_symbols)}")
    
    if pipeline_status.errors:
        logger.info("Errors encountered:")
        for error in pipeline_status.errors[:5]:  # Show first 5 errors
            logger.info(f"  - {error}")
        if len(pipeline_status.errors) > 5:
            logger.info(f"  ... and {len(pipeline_status.errors) - 5} more")
    
    logger.info("=" * 60)
    
    return len(pipeline_status.successful_symbols) > 0


def show_statistics() -> None:
    """Show current pipeline statistics."""
    logger = logging.getLogger(__name__)
    
    logger.info("Fetching pipeline statistics...")
    
    stats = data_processor.get_pipeline_statistics()
    
    if 'error' in stats:
        logger.error(f"Failed to get statistics: {stats['error']}")
        return
    
    logger.info("=" * 60)
    logger.info("CURRENT PIPELINE STATISTICS")
    logger.info("=" * 60)
    logger.info(f"Total Records: {stats.get('total_records', 0):,}")
    logger.info(f"Unique Symbols: {stats.get('unique_symbols', 0)}")
    logger.info(f"Configured Symbols: {len(stats.get('configured_symbols', []))}")
    
    coverage = stats.get('coverage', {})
    logger.info(f"Symbols with Data: {coverage.get('symbols_with_data', 0)}")
    
    if coverage.get('missing_symbols'):
        logger.warning(f"Missing Symbols: {', '.join(coverage['missing_symbols'])}")
    
    if coverage.get('extra_symbols'):
        logger.info(f"Extra Symbols: {', '.join(coverage['extra_symbols'])}")
    
    # Show per-symbol summary
    symbols_summary = stats.get('symbols_summary', {})
    if symbols_summary:
        logger.info("\nPer-Symbol Summary:")
        logger.info("-" * 60)
        for symbol, info in symbols_summary.items():
            logger.info(f"{symbol}: {info['record_count']:,} records, "
                       f"latest: {info['latest_date']}")
    
    logger.info("=" * 60)


def cleanup_data(days: int = 365) -> None:
    """Clean up old data."""
    logger = logging.getLogger(__name__)
    
    logger.info(f"Cleaning up data older than {days} days...")
    
    deleted_count = data_processor.cleanup_old_data(days)
    logger.info(f"Cleanup completed: {deleted_count} records deleted")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Stock Data Pipeline')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO', help='Set logging level')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Test command
    subparsers.add_parser('test', help='Test system connections')
    
    # Run command
    run_parser = subparsers.add_parser('run', help='Run the pipeline')
    run_parser.add_argument('--type', choices=['daily', 'intraday'],
                           default='daily', help='Data type to fetch')
    run_parser.add_argument('--symbols', nargs='+',
                           help='Specific symbols to process (default: all configured)')
    
    # Stats command
    subparsers.add_parser('stats', help='Show pipeline statistics')
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Clean up old data')
    cleanup_parser.add_argument('--days', type=int, default=365,
                               help='Days of data to keep (default: 365)')
    
    args = parser.parse_args()
    
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    if not args.command:
        parser.print_help()
        return 1
    
    try:
        if args.command == 'test':
            success = test_connections()
            return 0 if success else 1
        
        elif args.command == 'run':
            success = run_pipeline(args.type, args.symbols)
            return 0 if success else 1
        
        elif args.command == 'stats':
            show_statistics()
            return 0
        
        elif args.command == 'cleanup':
            cleanup_data(args.days)
            return 0
        
        else:
            parser.print_help()
            return 1
    
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        return 1
    
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())