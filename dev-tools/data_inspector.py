#!/usr/bin/env python3
"""
Development Helper: Data Inspector
===================================
Inspects CSV and Parquet files to identify potential data issues.

Usage:
    python dev-tools/data_inspector.py file1.csv file2.parquet
    python dev-tools/data_inspector.py file1.csv file2.csv --exclude-cols location,value
"""

import argparse
import sys
from pathlib import Path
import pandas as pd
import numpy as np


def get_project_root():
    """Get the project root directory (where this script's parent is)."""
    return Path(__file__).parent.parent


def load_file(file_path):
    """Load a CSV or Parquet file into a pandas DataFrame."""
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    if file_path.suffix.lower() == '.csv':
        return pd.read_csv(file_path)
    elif file_path.suffix.lower() in ['.parquet', '.pq']:
        return pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_path.suffix}")


def analyze_dataframe(df, file_name, exclude_cols=None):
    """Comprehensive analysis of a DataFrame."""
    exclude_cols = exclude_cols or []
    
    print("=" * 80)
    print(f"FILE: {file_name}")
    print("=" * 80)
    print()
    
    # Basic Info
    print("BASIC INFORMATION")
    print("-" * 80)
    print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    print()
    
    # Column Order
    print("COLUMN ORDER")
    print("-" * 80)
    print(f"Columns: {list(df.columns)}")
    print()
    
    # Data Types
    print("DATA TYPES")
    print("-" * 80)
    for col in df.columns:
        dtype = df[col].dtype
        null_count = df[col].isna().sum()
        null_pct = (null_count / len(df)) * 100
        print(f"  {col:25} → {str(dtype):15} (nulls: {null_count:>6} = {null_pct:>5.2f}%)")
    print()
    
    # Head Preview
    print("HEAD (first 10 rows)")
    print("-" * 80)
    print(df.head(10).to_string())
    print()
    
    # Tail Preview
    print("TAIL (last 5 rows)")
    print("-" * 80)
    print(df.tail(5).to_string())
    print()
    
    # Unique Values Analysis
    print("UNIQUE VALUES ANALYSIS")
    print("-" * 80)
    for col in df.columns:
        if col in exclude_cols:
            print(f"  {col:25} → [EXCLUDED]")
            continue
        
        unique_count = df[col].nunique()
        total_count = len(df)
        
        if unique_count <= 50:  # Show unique values if reasonable
            unique_vals = df[col].unique()
            if df[col].dtype == 'object':
                # Sort strings
                try:
                    unique_vals = sorted(unique_vals, key=lambda x: (x is None, x))
                except:
                    pass
            else:
                # Sort numbers
                try:
                    unique_vals = sorted(unique_vals)
                except:
                    pass
            
            print(f"  {col:25} → {unique_count:>6} unique values")
            print(f"    {unique_vals[:100]}")  # Show first 100 to avoid overflow
            if len(unique_vals) > 100:
                print(f"    ... and {len(unique_vals) - 100} more")
        else:
            print(f"  {col:25} → {unique_count:>6} unique values (too many to display)")
            # Show a sample
            sample = df[col].value_counts().head(10)
            print(f"    Top 10 most frequent:")
            for val, count in sample.items():
                print(f"      {val}: {count}")
    print()
    
    # Duplicate Analysis
    print("DUPLICATE ANALYSIS")
    print("-" * 80)
    dup_count = df.duplicated().sum()
    print(f"  Fully duplicate rows: {dup_count:,} ({(dup_count/len(df)*100):.2f}%)")
    
    # Check for duplicates on key columns combinations
    if len(df.columns) >= 2:
        # Try common key combinations
        key_combos = []
        
        # Add some intelligent guesses
        cols = df.columns.tolist()
        if 'location' in cols and 'reference_date' in cols and 'horizon' in cols:
            key_combos.append(['location', 'reference_date', 'horizon', 'target', 'output_type_id'])
        
        # Check each combo
        for combo in key_combos:
            if all(c in df.columns for c in combo):
                dup_on_keys = df.duplicated(subset=combo).sum()
                if dup_on_keys > 0:
                    print(f"    Duplicates on {combo}: {dup_on_keys:,}")
    print()
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    # Data Quality Checks
    print("DATA QUALITY CHECKS")
    print("-" * 80)
    
    # Check for mixed types in object columns
    for col in df.select_dtypes(include=['object']).columns:
        types_in_col = df[col].dropna().apply(type).unique()
        if len(types_in_col) > 1:
            print(f"    Column '{col}' has mixed types: {types_in_col}")
    
    # Check for unusual values
    for col in numeric_cols:
        inf_count = np.isinf(df[col]).sum()
        if inf_count > 0:
            print(f"    Column '{col}' has {inf_count} infinite values")
        
        negative_count = (df[col] < 0).sum()
        if negative_count > 0 and 'horizon' not in col:  # horizon can be negative
            print(f"    Column '{col}' has {negative_count} negative values")
    
    # Check for potential string/numeric confusion
    for col in df.select_dtypes(include=['object']).columns:
        if col in exclude_cols:
            continue
        sample = df[col].dropna().head(100)
        numeric_pattern = sample.apply(lambda x: str(x).replace('.', '').replace('-', '').isdigit())
        if numeric_pattern.sum() / len(sample) > 0.8:  # 80% look like numbers
            print(f"    Column '{col}' is type 'object' but contains numeric-looking strings")
            print(f"      Sample values: {sample.head(5).tolist()}")
    
    print()


def compare_files(df1, file1_name, df2, file2_name):
    """Compare two DataFrames and highlight differences."""
    print()
    print("=" * 80)
    print("COMPARISON BETWEEN FILES")
    print("=" * 80)
    print()
    
    # Column comparison
    print("COLUMN COMPARISON")
    print("-" * 80)
    cols1 = set(df1.columns)
    cols2 = set(df2.columns)
    
    if cols1 == cols2:
        if list(df1.columns) == list(df2.columns):
            print("  Both files have identical columns in the same order")
            print(f"     Columns: {list(df1.columns)}")
        else:
            print("  Both files have the same columns but in DIFFERENT ORDER")
            print(f"     File 1: {list(df1.columns)}")
            print(f"     File 2: {list(df2.columns)}")
    else:
        print("  Files have DIFFERENT columns")
        only_in_1 = cols1 - cols2
        only_in_2 = cols2 - cols1
        common = cols1 & cols2
        
        print(f"     Common columns ({len(common)}): {sorted(common)}")
        if only_in_1:
            print(f"     Only in file 1 ({len(only_in_1)}): {sorted(only_in_1)}")
        if only_in_2:
            print(f"     Only in file 2 ({len(only_in_2)}): {sorted(only_in_2)}")
    print()
    
    # Data type comparison for common columns
    common_cols = cols1 & cols2
    if common_cols:
        print("DATA TYPE COMPARISON (common columns)")
        print("-" * 80)
        type_diffs = []
        for col in sorted(common_cols):
            dtype1 = str(df1[col].dtype)
            dtype2 = str(df2[col].dtype)
            if dtype1 != dtype2:
                type_diffs.append((col, dtype1, dtype2))
                print(f"    '{col}': {dtype1} (file1) vs {dtype2} (file2)")
        
        if not type_diffs:
            print("  All common columns have matching data types")
        print()
    
    # Shape comparison
    print("SHAPE COMPARISON")
    print("-" * 80)
    print(f"  File 1: {df1.shape[0]:,} rows × {df1.shape[1]} columns")
    print(f"  File 2: {df2.shape[0]:,} rows × {df2.shape[1]} columns")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Inspect CSV/Parquet files for data quality issues",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python dev-tools/data_inspector.py file1.csv file2.csv
  python dev-tools/data_inspector.py file1.csv file2.csv --exclude-cols location,value,date
  python dev-tools/data_inspector.py development-mode-root/model-output/Model1/file.csv
        """
    )
    
    parser.add_argument(
        'files',
        nargs='+',
        help='Path to files to inspect (relative to project root). Supports CSV and Parquet.'
    )
    
    parser.add_argument(
        '--exclude-cols',
        type=str,
        default='',
        help='Comma-separated list of columns to exclude from unique value analysis (e.g., "location,value,date")'
    )
    
    args = parser.parse_args()
    
    # Parse exclude columns
    exclude_cols = [col.strip() for col in args.exclude_cols.split(',') if col.strip()]
    
    # Get project root
    project_root = get_project_root()
    
    # Load and analyze files
    dataframes = []
    for file_path_str in args.files:
        # Convert to absolute path if relative
        file_path = Path(file_path_str)
        if not file_path.is_absolute():
            file_path = project_root / file_path
        
        try:
            print(f"\nLoading {file_path}...")
            df = load_file(file_path)
            dataframes.append((df, file_path.name))
            
            analyze_dataframe(df, file_path.name, exclude_cols)
            
        except Exception as e:
            print(f"Error loading {file_path}: {e}", file=sys.stderr)
            sys.exit(1)
    
    # If two files provided, do a comparison
    if len(dataframes) == 2:
        compare_files(dataframes[0][0], dataframes[0][1], 
                     dataframes[1][0], dataframes[1][1])
    
    print()
    print("=" * 80)
    print("Analysis complete!")
    print("=" * 80)


if __name__ == '__main__':
    main()
