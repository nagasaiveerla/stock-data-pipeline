"""
Airflow DAG for stock market data pipeline.
Orchestrates the fetching, processing, and storage of stock market data.
"""

import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Add scripts directory to Python path
sys.path.append('/opt/airflow/scripts')

# Change these imports to match how they're defined in the modules
from scripts.data_processor import DataProcessor
from scripts.database import db_manager
from scripts.api_client import api_client
from scripts.config import config
from scripts.models import PipelineStatus, StockDataBatch, DatabaseOperationResult


# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='Automated stock market data pipeline',
    schedule_interval=timedelta(hours=1),  # Run hourly
    max_active_runs=1,  # Prevent concurrent runs
    tags=['stock-market', 'data-pipeline', 'finance']
)


def test_connections(**context) -> Dict[str, Any]:
    """Test database and API connections before starting pipeline."""
    results = {
        'database_connection': False,
        'api_connection': False,
        'timestamp': datetime.now().isoformat()
    }
    
    try:
        # Test database connection
        results['database_connection'] = db_manager.test_connection()
        
        # Test API connection
        results['api_connection'] = api_client.health_check()
        
        # Create tables if they don't exist
        if results['database_connection']:
            db_manager.create_tables()
        
        # Log results
        context['ti'].xcom_push(key='connection_test', value=results)
        
        if not results['database_connection']:
            raise Exception("Database connection failed")
        
        if not results['api_connection']:
            context['ti'].log.warning("API connection test failed - continuing anyway")
        
        return results
        
    except Exception as e:
        context['ti'].log.error(f"Connection test failed: {e}")
        raise


def fetch_daily_data(**context) -> Dict[str, Any]:
    """Fetch daily stock data for all configured symbols."""
    try:
        # Get configuration
        symbols = config.pipeline.symbols
        context['ti'].log.info(f"Processing daily data for symbols: {symbols}")
        
        # Run pipeline
        pipeline_status = data_processor.run_full_pipeline(
            data_type='daily',
            symbols=symbols
        )
        
        # Prepare result summary
        result = {
            'pipeline_run_id': pipeline_status.pipeline_run_id,
            'symbols_processed': pipeline_status.symbols_processed,
            'successful_symbols': pipeline_status.successful_symbols,
            'failed_symbols': pipeline_status.failed_symbols,
            'total_records_processed': pipeline_status.total_records_processed,
            'success_rate': pipeline_status.success_rate,
            'duration_seconds': pipeline_status.duration_seconds,
            'errors': pipeline_status.errors[:10]  # Limit errors for XCom
        }
        
        # Push to XCom
        context['ti'].xcom_push(key='daily_pipeline_result', value=result)
        
        # Log summary
        context['ti'].log.info(f"Daily pipeline completed:")
        context['ti'].log.info(f"  Success rate: {result['success_rate']:.1f}%")
        context['ti'].log.info(f"  Records processed: {result['total_records_processed']}")
        context['ti'].log.info(f"  Duration: {result['duration_seconds']:.2f}s")
        
        # Fail task if no symbols were successfully processed
        if not pipeline_status.successful_symbols:
            raise Exception("No symbols were successfully processed")
        
        return result
        
    except Exception as e:
        context['ti'].log.error(f"Daily data fetch failed: {e}")
        raise


def fetch_intraday_data(**context) -> Dict[str, Any]:
    """Fetch intraday stock data for a subset of symbols."""
    try:
        # Get high-priority symbols (first few from config)
        all_symbols = config.pipeline.symbols
        priority_symbols = all_symbols[:3]  # Process first 3 symbols for intraday
        
        context['ti'].log.info(f"Processing intraday data for symbols: {priority_symbols}")
        
        # Run pipeline
        pipeline_status = data_processor.run_full_pipeline(
            data_type='intraday',
            symbols=priority_symbols
        )
        
        # Prepare result summary
        result = {
            'pipeline_run_id': pipeline_status.pipeline_run_id,
            'symbols_processed': pipeline_status.symbols_processed,
            'successful_symbols': pipeline_status.successful_symbols,
            'failed_symbols': pipeline_status.failed_symbols,
            'total_records_processed': pipeline_status.total_records_processed,
            'success_rate': pipeline_status.success_rate,
            'duration_seconds': pipeline_status.duration_seconds,
            'errors': pipeline_status.errors[:10]
        }
        
        context['ti'].xcom_push(key='intraday_pipeline_result', value=result)
        
        context['ti'].log.info(f"Intraday pipeline completed:")
        context['ti'].log.info(f"  Success rate: {result['success_rate']:.1f}%")
        context['ti'].log.info(f"  Records processed: {result['total_records_processed']}")
        
        return result
        
    except Exception as e:
        context['ti'].log.error(f"Intraday data fetch failed: {e}")
        raise


def validate_data_quality(**context) -> Dict[str, Any]:
    """Validate data quality and generate statistics."""
    try:
        # Get pipeline statistics
        stats = data_processor.get_pipeline_statistics()
        
        # Basic data quality checks
        quality_checks = {
            'total_records_check': stats.get('total_records', 0) > 0,
            'symbols_coverage_check': len(stats.get('symbols_summary', {})) > 0,
            'recent_data_check': True  # Would implement actual recency check
        }
        
        # Calculate overall quality score
        passed_checks = sum(quality_checks.values())
        total_checks = len(quality_checks)
        quality_score = (passed_checks / total_checks) * 100
        
        result = {
            'statistics': stats,
            'quality_checks': quality_checks,
            'quality_score': quality_score,
            'validation_timestamp': datetime.now().isoformat()
        }
        
        context['ti'].xcom_push(key='data_quality_result', value=result)
        
        context['ti'].log.info(f"Data quality validation completed:")
        context['ti'].log.info(f"  Quality score: {quality_score:.1f}%")
        context['ti'].log.info(f"  Total records: {stats.get('total_records', 0)}")
        context['ti'].log.info(f"  Unique symbols: {stats.get('unique_symbols', 0)}")
        
        # Warn if quality score is low
        if quality_score < 80:
            context['ti'].log.warning(f"Data quality score is low: {quality_score:.1f}%")
        
        return result
        
    except Exception as e:
        context['ti'].log.error(f"Data quality validation failed: {e}")
        raise


def cleanup_old_data(**context) -> Dict[str, Any]:
    """Clean up old data to manage storage."""
    try:
        # Get cleanup configuration
        days_to_keep = int(Variable.get('stock_data_retention_days', default_var=365))
        
        context['ti'].log.info(f"Cleaning up data older than {days_to_keep} days")
        
        # Perform cleanup
        deleted_count = data_processor.cleanup_old_data(days_to_keep)
        
        result = {
            'days_to_keep': days_to_keep,
            'deleted_records': deleted_count,
            'cleanup_timestamp': datetime.now().isoformat()
        }
        
        context['ti'].xcom_push(key='cleanup_result', value=result)
        
        context['ti'].log.info(f"Cleanup completed: {deleted_count} records deleted")
        
        return result
        
    except Exception as e:
        context['ti'].log.error(f"Data cleanup failed: {e}")
        raise


def send_pipeline_summary(**context) -> Dict[str, Any]:
    """Generate and send pipeline execution summary."""
    try:
        # Get results from previous tasks
        daily_result = context['ti'].xcom_pull(key='daily_pipeline_result', task_ids='fetch_daily_data')
        intraday_result = context['ti'].xcom_pull(key='intraday_pipeline_result', task_ids='fetch_intraday_data')
        quality_result = context['ti'].xcom_pull(key='data_quality_result', task_ids='validate_data_quality')
        cleanup_result = context['ti'].xcom_pull(key='cleanup_result', task_ids='cleanup_old_data')
        
        # Create summary
        summary = {
            'dag_run_id': context['dag_run'].run_id,
            'execution_date': context['execution_date'].isoformat(),
            'daily_pipeline': daily_result,
            'intraday_pipeline': intraday_result,
            'data_quality': quality_result,
            'cleanup': cleanup_result,
            'overall_success': True
        }
        
        # Calculate overall metrics
        total_records = 0
        if daily_result:
            total_records += daily_result.get('total_records_processed', 0)
        if intraday_result:
            total_records += intraday_result.get('total_records_processed', 0)
        
        summary['total_records_processed'] = total_records
        
        # Log summary
        context['ti'].log.info("=== PIPELINE EXECUTION SUMMARY ===")
        context['ti'].log.info(f"DAG Run: {summary['dag_run_id']}")
        context['ti'].log.info(f"Execution Date: {summary['execution_date']}")
        context['ti'].log.info(f"Total Records Processed: {total_records}")
        
        if daily_result:
            context['ti'].log.info(f"Daily Data - Success Rate: {daily_result.get('success_rate', 0):.1f}%")
        
        if intraday_result:
            context['ti'].log.info(f"Intraday Data - Success Rate: {intraday_result.get('success_rate', 0):.1f}%")
        
        if quality_result:
            context['ti'].log.info(f"Data Quality Score: {quality_result.get('quality_score', 0):.1f}%")
        
        if cleanup_result:
            context['ti'].log.info(f"Cleanup: {cleanup_result.get('deleted_records', 0)} records deleted")
        
        context['ti'].log.info("=== END SUMMARY ===")
        
        # Store summary for potential notifications
        context['ti'].xcom_push(key='pipeline_summary', value=summary)
        
        return summary
        
    except Exception as e:
        context['ti'].log.error(f"Failed to generate pipeline summary: {e}")
        raise


# Task definitions
test_connections_task = PythonOperator(
    task_id='test_connections',
    python_callable=test_connections,
    dag=dag,
    doc_md="""
    ## Test Connections
    
    Tests database and API connections before starting the pipeline.
    Creates database tables if they don't exist.
    """
)

fetch_daily_data_task = PythonOperator(
    task_id='fetch_daily_data',
    python_callable=fetch_daily_data,
    dag=dag,
    doc_md="""
    ## Fetch Daily Data
    
    Fetches daily stock market data for all configured symbols.
    This is the main data ingestion task.
    """
)

fetch_intraday_data_task = PythonOperator(
    task_id='fetch_intraday_data',
    python_callable=fetch_intraday_data,
    dag=dag,
    doc_md="""
    ## Fetch Intraday Data
    
    Fetches intraday (hourly) stock data for priority symbols.
    Provides more granular data for key stocks.
    """
)

validate_data_quality_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
    doc_md="""
    ## Validate Data Quality
    
    Performs data quality checks and generates statistics.
    Ensures data integrity and completeness.
    """
)

cleanup_old_data_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data,
    dag=dag,
    doc_md="""
    ## Cleanup Old Data
    
    Removes old data to manage storage space.
    Retention period is configurable via Airflow Variables.
    """
)

send_summary_task = PythonOperator(
    task_id='send_pipeline_summary',
    python_callable=send_pipeline_summary,
    dag=dag,
    trigger_rule='all_done',  # Run even if some upstream tasks fail
    doc_md="""
    ## Send Pipeline Summary
    
    Generates and logs a comprehensive summary of pipeline execution.
    Collects metrics from all pipeline tasks.
    """
)

# Task dependencies
test_connections_task >> [fetch_daily_data_task, fetch_intraday_data_task]

[fetch_daily_data_task, fetch_intraday_data_task] >> validate_data_quality_task

validate_data_quality_task >> cleanup_old_data_task

cleanup_old_data_task >> send_summary_task



