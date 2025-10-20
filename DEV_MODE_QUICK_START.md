# Dev Mode Quick Start

## How To Use

```bash
# Run data processor in dev mode
python scripts/test_data_processor.py

# Or with the full workflow
python scripts/dashboard_builder_workflow.py --dev
```

**Put Your Input Data In:** `test-data-input/`  
**Generated Output In:** `test-data-output/`  

## Quick Example

```python
from yaml_config_processor import load_dashboard_config
from data_processor import process_data

# Load your config
config = load_dashboard_config("config.yaml")

# Run in dev mode - outputs to test-data-output/
process_data(config, dev_mode=True)

# Run in production mode - outputs to public/data/
process_data(config, dev_mode=False)
```

### No files in output?
- Check if `config.yaml` exists and is valid
- Verify input data exists in `test-data-input/`
- Look for error messages in the logs

### Model showing as "skipping"?
- Model folder name must match exactly (case-sensitive)
- Check `test-data-input/model-output/MODEL-NAME/` exists

### Empty prediction files?
- Check forecast period dates in `config.yaml`
- Ensure `reference_date` in model files overlap with periods
- Verify column mappings are correct
