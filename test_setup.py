#!/usr/bin/env python3
"""
Test script to verify the stock data pipeline setup.
Run this script to check if all components are working correctly.
"""

import os
import sys
import logging
from datetime import datetime

# Add scripts directory to path
sys.path.append('./scripts')

def test_imports():
    """Test if all required modules can be imported."""
    print("Testing imports...")
    
    try:
        import scripts.config as config_module
        print("✓ Config module imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import config: {e}")
        return False
    except ValueError as e:
        print(f"⚠ Config module imported but environment variables missing: {e}")
        # Continue with other tests even if config fails
    
    try:
        import scripts.models as models_module
        print("✓ Models module imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import models: {e}")
        return False
    
    try:
        import scripts.database as database_module
        print("✓ Database module imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import database: {e}")
        return False
    
    try:
        import scripts.api_client as api_client_module
        print("✓ API client module imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import api_client: {e}")
        return False
    
    try:
        import scripts.data_processor as data_processor_module
        print("✓ Data processor module imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import data_processor: {e}")
        return False
    
    return True

def test_configuration():
    """Test configuration loading."""
    print("\nTesting configuration...")
    
    try:
        import scripts.config as config_module
        
        # Check if API key is set
        api_key = config_module.config.api.alpha_vantage_key
        if api_key and api_key != 'demo':
            print("✓ API key is configured")
        else:
            print("⚠ API key not set (using demo key)")
        
        # Check database configuration
        db_config = config_module.config.database
        print(f"✓ Database config: {db_config.host}:{db_config.port}/{db_config.database}")
        
        # Check pipeline configuration
        pipeline_config = config_module.config.pipeline
        print(f"✓ Pipeline config: {len(pipeline_config.symbols)} symbols, {pipeline_config.max_workers} workers")
        
        return True
        
    except Exception as e:
        print(f"✗ Configuration test failed: {e}")
        return False

def test_models():
    """Test data models."""
    print("\nTesting data models...")
    
    try:
        import scripts.models as models_module
        
        # Test StockDataPoint creation
        data_point = models_module.StockDataPoint(
            symbol="AAPL",
            timestamp=datetime.now(),
            open_price=150.0,
            high_price=155.0,
            low_price=149.0,
            close_price=152.0,
            volume=1000000
        )
        print("✓ StockDataPoint model works correctly")
        
        # Test PipelineStatus creation
        status = models_module.PipelineStatus(
            pipeline_run_id="test-run-123",
            start_time=datetime.now(),
            symbols_processed=["AAPL", "GOOGL"],
            successful_symbols=["AAPL"],
            failed_symbols=["GOOGL"]
        )
        print("✓ PipelineStatus model works correctly")
        
        return True
        
    except Exception as e:
        print(f"✗ Model test failed: {e}")
        return False

def main():
    """Main test function."""
    print("=" * 60)
    print("STOCK DATA PIPELINE SETUP TEST")
    print("=" * 60)
    print(f"Test time: {datetime.now()}")
    print()
    
    # Test imports
    if not test_imports():
        print("\n❌ Import tests failed. Check your Python environment and dependencies.")
        return 1
    
    # Test configuration
    if not test_configuration():
        print("\n❌ Configuration tests failed. Check your environment variables.")
        return 1
    
    # Test models
    if not test_models():
        print("\n❌ Model tests failed. Check your data models.")
        return 1
    
    print("\n" + "=" * 60)
    print("✅ ALL TESTS PASSED!")
    print("=" * 60)
    print("\nYour stock data pipeline setup is ready!")
    print("\nNext steps:")
    print("1. Set your ALPHA_VANTAGE_API_KEY in .env file")
    print("2. Run: docker-compose up -d")
    print("3. Access Airflow UI at http://localhost:8080")
    print("4. Enable the 'stock_data_pipeline' DAG")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
