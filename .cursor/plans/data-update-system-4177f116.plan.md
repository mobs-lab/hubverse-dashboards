<!-- 4177f116-0cdd-469d-b40e-465e2fcebddf 538017a0-0ffe-4707-8ad0-3350fcd525bb -->
# Incremental Data Update & Configuration Improvements

## Part 1: Fix Prediction Intervals (HIGH PRIORITY)

### Issue: Asymmetric prediction intervals in config.yaml

**Current (WRONG):**

```yaml
- "25": ["0.1", "0.35"]   # 10th-35th percentile (left-skewed)
- "75": ["0.15", "0.9"]   # 15th-90th percentile (right-skewed)
```

**Should be (CENTERED):**

```yaml
- "25": ["0.375", "0.625"]  # 37.5th-62.5th percentile
- "75": ["0.125", "0.875"]  # 12.5th-87.5th percentile
```

**Formula**: For C% interval centered on median (0.5):

- Lower = 0.5 - C/200 = (1 - C/100)/2
- Upper = 0.5 + C/200

**Actions:**

1. Add validation in `yaml_config_processor.py` `_parse_prediction_intervals()`:

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                - Check that intervals are symmetric around 0.5
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                - Calculate expected quantiles and warn if mismatch
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                - Log warning but allow user override

2. Update `config.yaml.example` with correct examples and formula explanation

3. Create `PREDICTION_INTERVALS_GUIDE.md` explaining:

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                - Mathematical basis for symmetric intervals
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                - Valid quantile combinations table
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                - Why asymmetric intervals are problematic
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                - How they're used in WIS and coverage calculations

---

## Part 2: Single File Name Support

### Issue: `single_target_data_file_name` config not being used

**File:** `scripts/data_processor.py` `_load_target_data()` method

**Current code (lines 127-160):**

```python
if file_format == "csv":
    csv_file = next(self.target_data_path.glob("*.csv"))  # Takes first CSV found
elif file_format == "parquet":
    if is_partitioned:
        df = self._load_partitioned_parquet()
    else:
        parquet_file = parquet_files[0]  # Takes first parquet found
```

**Changes needed:**

1. Add method to get configured filename:
```python
def _get_target_data_filename(self) -> str:
    """Get configured target data filename or None for auto-detect."""
    filename = self.config._get_value("single_target_data_file_name")
    if filename:
        file_format = self.config.target_data_file_format
        return f"{filename}.{file_format}"
    return None
```

2. Update CSV loading (line ~129):
```python
configured_name = self._get_target_data_filename()
if configured_name:
    csv_file = self.target_data_path / configured_name
    if not csv_file.exists():
        raise FileNotFoundError(f"Configured file not found: {csv_file}")
else:
    csv_file = next(self.target_data_path.glob("*.csv"))
```

3. Update parquet loading (line ~155):
```python
configured_name = self._get_target_data_filename()
if configured_name:
    parquet_file = self.target_data_path / configured_name
    if not parquet_file.exists():
        raise FileNotFoundError(f"Configured file not found: {parquet_file}")
else:
    # Auto-detect first parquet file
```


---

## Part 3: Improve Partitioned Parquet Handling

### Current implementation already handles multiple files per partition!

**Clarify documentation and add logging:**

**File:** `scripts/data_processor.py` `_load_partitioned_parquet()` (lines 208-310)

**Changes:**

1. Improve docstring (line 209):
```python
"""
Load partitioned parquet files (Hive-style partitioning).

Expected directory structure (multiple files per partition supported):
    target-data/
      as_of=2024-01-01/          # Hive-style (REQUIRED format)
        data.parquet
        additional.parquet         # Multiple files OK
      as_of=2024-01-08/
        part-0.parquet
        part-1.parquet
      ...

Note: Only Hive-style partitioning (as_of=YYYY-MM-DD) is supported.

Returns:
    Combined DataFrame with as_of column added from partition directory names
"""
```

2. Simplify date parsing to only support Hive-style (lines 245-262):
```python
# Extract as_of date from Hive-style partition: as_of=YYYY-MM-DD
match = re.match(r"as_of=(\d{4}-\d{2}-\d{2})", dir_name)
if not match:
    logger.warning(f"  ⚠ Directory '{dir_name}' is not Hive-style (as_of=YYYY-MM-DD), skipping")
    continue

as_of_date = match.group(1)
```

3. Add logging for multiple files per partition (after line 276):
```python
parquet_files = list(subdir.glob("*.parquet")) + list(subdir.glob("*.pq"))

if not parquet_files:
    logger.warning(f"  ⚠ No parquet files found in {dir_name}, skipping")
    continue

if len(parquet_files) > 1:
    logger.info(f"  → Found {len(parquet_files)} parquet files in partition {dir_name}")
```


---

## Part 4: Data Update Workflow System

### Architecture: Hybrid Change Detection + Incremental Evaluation

**New file:** `scripts/update_tracker.py`

### 4.1 State Manifest Structure

**File:** `.dashboard_state.json` (created in project root)

```json
{
  "last_run_timestamp": "2024-10-20T15:30:00Z",
  "target_data": {
    "file_path": "target-data/time-series.parquet",
    "file_hash": "abc123...",
    "last_modified": "2024-10-20T14:00:00Z",
    "row_count": 125000,
    "latest_date": "2024-10-15"
  },
  "model_output": {
    "MOBS-GLEAM_COVID": {
      "files": {
        "2024-10-01-MOBS-GLEAM_COVID.csv": {
          "hash": "def456...",
          "added": "2024-10-01T10:00:00Z"
        }
      },
      "latest_reference_date": "2024-10-01"
    }
  },
  "processed_periods": [
    "season-2024-2025",
    "last-2-weeks"
  ],
  "evaluation_keys": [
    ["MOBS-GLEAM_COVID", "06", 1, "2024-10-01", "2024-10-08"],
    ...
  ]
}
```

### 4.2 UpdateTracker Class

**File:** `scripts/update_tracker.py` (~300 lines)

```python
class UpdateTracker:
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.manifest_path = project_root / ".dashboard_state.json"
        self.manifest = self._load_manifest()
    
    def _load_manifest(self) -> dict:
        """Load existing manifest or create new one."""
        
    def _compute_file_hash(self, file_path: Path) -> str:
        """Compute SHA256 hash of file for change detection."""
        
    def detect_target_data_changes(self, target_data_path: Path, current_df: pd.DataFrame) -> dict:
        """
        Hybrid detection for target-data:
 1. File-level: Check hash to see if file changed
 2. Row-level: If changed, compare DataFrames to find new/modified rows
        
        Returns:
            {
                "changed": bool,
                "new_rows": pd.DataFrame,  # Rows not in previous version
                "modified_dates": list,     # Date values that changed
                "modified_locations": list  # Location codes that changed
            }
        """
        
    def detect_model_output_changes(self, model_output_path: Path) -> dict:
        """
        File-level detection for model-output:
        Scan each model's directory for new/modified files
        
        Returns:
            {
                "MOBS-GLEAM_COVID": {
                    "new_files": ["2024-10-15-MOBS-GLEAM_COVID.csv"],
                    "modified_files": []
                }
            }
        """
        
    def identify_affected_periods(self, target_changes: dict, model_changes: dict, 
                                  config: DashboardConfig) -> list:
        """
        Determine which forecast periods need recalculation based on:
 - Changed date ranges in target-data
 - New reference dates in model-output
 - Dynamic period dependencies
        
        Returns:
            ["season-2024-2025", "last-2-weeks", ...]
        """
        
    def save_manifest(self, target_df: pd.DataFrame, model_files: dict, 
                     processed_periods: list, evaluation_keys: list):
        """Update and save manifest after successful processing."""
```

### 4.3 Incremental Evaluation Loading

**File:** `scripts/evaluation_loader.py` (~200 lines)

```python
class EvaluationLoader:
    def __init__(self, output_base_path: Path):
        self.output_base_path = output_base_path
    
    def load_existing_evaluations(self, period_id: str) -> dict:
        """
        Load existing evaluation files for a period.
        
        Returns:
            {
                'wis': pd.DataFrame or None,
                'wis_ratio': pd.DataFrame or None,
                'coverage': pd.DataFrame or None,
                'mape': pd.DataFrame or None
            }
        """
        
    def merge_evaluations(self, old_evals: dict, new_evals: dict) -> dict:
        """
        Merge new evaluations with existing using epistorm strategy:
        
        For each metric DataFrame:
 1. Identify truly new rows (not duplicates)
 2. Identify rows being replaced (same keys, different values)
 3. Remove replaced rows from old
 4. Concatenate old (retained) + new
        
        Keys for matching: [model, location, horizon, reference_date, target_end_date]
        """
        
    def calculate_merge_stats(self, old_evals: dict, new_evals: dict, 
                             merged_evals: dict) -> dict:
        """Return statistics about what changed for user reporting."""
```

### 4.4 Integration into DataProcessor

**File:** `scripts/data_processor.py`

**Add to `__init__` (line ~40):**

```python
from update_tracker import UpdateTracker
from evaluation_loader import EvaluationLoader

self.update_tracker = UpdateTracker(self.project_root) if not dev_mode else None
self.evaluation_loader = EvaluationLoader(self.output_base_path)
self.is_incremental_update = False
self.affected_periods = []
```

**New method (after line ~118):**

```python
def detect_and_plan_update(self) -> bool:
    """
    Detect changes and determine update strategy.
    
    Returns:
        True if incremental update possible, False if full rebuild needed
    """
    if self.dev_mode or not self.update_tracker:
        return False  # Always full build in dev mode
    
    # Check if manifest exists
    if not self.update_tracker.manifest_path.exists():
        logger.info("No previous run detected, performing full build")
        return False
    
    # Detect changes
    logger.info("Checking for data changes...")
    target_changes = self.update_tracker.detect_target_data_changes(
        self.target_data_path, target_data_df
    )
    model_changes = self.update_tracker.detect_model_output_changes(
        self.model_output_path
    )
    
    # Decide strategy
    if not target_changes["changed"] and not any(model_changes.values()):
        logger.info("✓ No changes detected, skipping processing")
        return None  # Signal: no processing needed
    
    # Identify affected periods
    self.affected_periods = self.update_tracker.identify_affected_periods(
        target_changes, model_changes, self.config
    )
    
    self.is_incremental_update = True
    logger.info(f"Incremental update for {len(self.affected_periods)} period(s): {self.affected_periods}")
    return True
```

**Modify `run()` method (line ~71):**

```python
def run(self):
    """Main entry point to run the data processing pipeline."""
    logger.info("Starting data processing...")
    
    # Step 0: Detect changes and plan update
    update_mode = self.detect_and_plan_update()
    if update_mode is None:
        logger.info("✓ Data is up to date, no processing needed")
        return True
    
    # Step 1: Load data (same as before)
    target_data_df = self._load_target_data()
    model_output_df = self._load_model_output_data()
    
    # Steps 2-5: Process data (same as before)
    # ...
    
    # Step 6: Calculate evaluations (modified for incremental)
    if self.is_incremental_update:
        evaluations_by_period = self._process_evaluations_incremental(
            fixed_target_data_df, model_output_df
        )
    else:
        evaluations_by_period = self._process_evaluations_by_periods(
            fixed_target_data_df, model_output_df
        )
    
    # Steps 7-8: Write output and print summary
    # ...
    
    # Step 9: Update manifest
    if self.update_tracker:
        self.update_tracker.save_manifest(
            target_data_df, model_files_info, 
            processed_periods, evaluation_keys
        )
    
    return True
```

**New method for incremental evaluations:**

```python
def _process_evaluations_incremental(self, target_data_df: pd.DataFrame, 
                                     model_output_df: pd.DataFrame) -> dict:
    """
    Process evaluations incrementally, only for affected periods.
    Merges with existing evaluation data.
    """
    evaluations_by_period = {}
    
    # Determine which periods to process
    if self.affected_periods:
        periods_to_process = [p for p in self.config.forecast_periods + self.config.dynamic_periods 
                             if p.period_id in self.affected_periods]
    else:
        periods_to_process = self.config.forecast_periods + self.config.dynamic_periods
    
    logger.info(f"Processing evaluations for {len(periods_to_process)} affected period(s)...")
    
    for period in periods_to_process:
        period_id = period.period_id
        
        # Load existing evaluations
        old_evals = self.evaluation_loader.load_existing_evaluations(period_id)
        
        # Calculate new evaluations (same as _process_evaluations_by_periods)
        # ... [period filtering and evaluation calculation logic] ...
        new_evals = self.evaluation_processor.evaluate_predictions(...)
        
        # Merge with existing
        if any(old_evals.values()):
            merged_evals = self.evaluation_loader.merge_evaluations(old_evals, new_evals)
            merge_stats = self.evaluation_loader.calculate_merge_stats(
                old_evals, new_evals, merged_evals
            )
            logger.info(f"  Merged evaluations for {period_id}:")
            logger.info(f"    • Retained: {merge_stats['retained']} existing")
            logger.info(f"    • Added: {merge_stats['added']} new")
            logger.info(f"    • Replaced: {merge_stats['replaced']} updated")
            evaluations_by_period[period_id] = merged_evals
        else:
            evaluations_by_period[period_id] = new_evals
    
    return evaluations_by_period
```

### 4.5 User-Facing Update Summary

**Modify `_print_processing_summary()` to show update info:**

```python
if self.is_incremental_update:
    logger.info("")
    logger.info("UPDATE MODE:")
    logger.info(f"  • Affected periods: {', '.join(self.affected_periods)}")
    logger.info(f"  • Evaluation merge statistics: {merge_stats_summary}")
```

---

## Part 5: Command-Line Interface Updates

**File:** `scripts/dashboard_builder_workflow.py`

**Add new argument (line ~240):**

```python
parser.add_argument(
    "--force-rebuild",
    action="store_true",
    help="Force full rebuild even if incremental update is possible",
)
```

**Pass to DataProcessor (line ~254):**

```python
builder = DashboardBuilder(config_path=args.config, dev_mode=args.dev)
# Pass force_rebuild flag
builder.force_rebuild = args.force_rebuild
```

---

## Implementation Order

1. **Prediction intervals fix** (30 min) - Critical correctness issue
2. **Single file name support** (20 min) - Simple addition
3. **Partitioned parquet improvements** (15 min) - Documentation only
4. **Update tracker core** (2-3 hours) - Foundation
5. **Evaluation loader** (1-2 hours) - Merging logic
6. **DataProcessor integration** (2-3 hours) - Connect pieces
7. **Testing & refinement** (2-3 hours) - Edge cases

**Total estimated time:** 8-12 hours of focused work

---

## Testing Strategy

1. **Unit tests** for UpdateTracker change detection
2. **Integration test**: Run full build, modify data, run incremental
3. **Validation**: Compare full rebuild vs incremental results (should match)
4. **Edge cases**: Empty updates, all data changed, only model-output changed

---

## Benefits

- ⚡ **Performance**: 50-70% faster for typical updates
- 🔍 **Transparency**: Clear reporting of what changed
- 🛡️ **Safety**: Validation and rollback capabilities
- 📊 **Efficiency**: Only recalculate what's needed
- 🎯 **Simplicity**: Automatic detection, no manual intervention

### To-dos

- [ ] Add validation for symmetric prediction intervals in yaml_config_processor.py and update config.yaml.example
- [ ] Implement single_target_data_file_name support in _load_target_data() method
- [ ] Improve partitioned parquet documentation and logging, simplify to Hive-style only
- [ ] Create update_tracker.py with UpdateTracker class for change detection
- [ ] Create evaluation_loader.py with EvaluationLoader class for merging evaluations
- [ ] Integrate update tracking and incremental evaluation into DataProcessor
- [ ] Add --force-rebuild flag and update CLI interface
- [ ] Create tests for update detection and evaluation merging