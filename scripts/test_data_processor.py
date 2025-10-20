#!/usr/bin/env python3
"""
Test script for running the DataProcessor in development mode.

This script allows you to test the data processing pipeline using the test data
in test-data-input/ and outputs to test-data-output/.

Usage:
    python scripts/test_data_processor.py
"""

import sys
from pathlib import Path

# Add the scripts directory to the path
sys.path.insert(0, str(Path(__file__).parent))

from yaml_config_processor import load_dashboard_config
from data_processor import process_data


def main():
    """Run the data processor in development mode."""
    project_root = Path(__file__).parent.parent
    config_file = project_root / "config.yaml"
    
    if not config_file.exists():
        print("❌ Error: config.yaml not found in project root!")
        print("Please copy config.yaml.example to config.yaml and configure it.")
        sys.exit(1)
    
    print("Loading configuration...")
    config = load_dashboard_config(str(config_file))
    
    print("\n" + "=" * 60)
    print(" 	RUNNING DATA PROCESSOR IN DEV MODE")
    print("=" * 60)
    print(f"Input:  test-data-input/")
    print(f"Output: test-data-output/")
    print("=" * 60 + "\n")
    
    try:
        process_data(config, dev_mode=True)
        print("\n✅ Test completed successfully!")
        print("\nCheck test-data-output/ for the generated files.")
        return 0
    except Exception as e:
        print(f"\n❌ Error during processing: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

