# Documentation Guide

This project uses **Sphinx** with **autodoc** and **Pydantic** support to automatically generate API documentation from Python code.

## Quick Start

### 1. Install Documentation Dependencies

```bash
pip install -r requirements-dev.txt
```

This installs:
- `sphinx` - Documentation generator
- `pydantic-sphinx` - Pydantic model documentation support
- `autodoc` - Automatic documentation from docstrings

### 2. Build the Documentation

```bash
cd docs
make html
```

The generated HTML documentation will be in `docs/build/html/`.

### 3. View the Documentation

Open `docs/build/html/index.html` in your browser:

```bash
open docs/build/html/index.html  # macOS
```

Or start a simple HTTP server:

```bash
cd docs/build/html
python -m http.server 8000
# Visit http://localhost:8000
```

## How It Works

### Configuration

The Sphinx configuration is in `docs/source/conf.py` with these key settings:

- **autodoc**: Automatically generates docs from Python docstrings
- **napoleon**: Supports Google/NumPy style docstrings
- **autodoc_pydantic**: Special support for Pydantic models
- **myst_parser**: Allows writing docs in Markdown

### Documenting Python Code

#### Regular Functions/Classes

Use standard docstrings:

```python
def process_data(data: dict) -> list:
    """
    Process input data and return results.
    
    Args:
        data: Input data dictionary
        
    Returns:
        List of processed results
    """
    pass
```

#### Pydantic Models

Pydantic models are automatically documented with their fields, types, and descriptions:

```python
from pydantic import BaseModel, Field

class DashboardConfig(BaseModel):
    """Main dashboard configuration."""
    
    title: str = Field(..., description="Dashboard title")
    target_variable: str = Field(..., description="Target variable name")
    start_date: str = Field(..., description="Start date in YYYY-MM-DD format")
```

The autodoc_pydantic extension will automatically show:
- Field names and types
- Field descriptions
- Default values
- JSON schema (optional)

### Adding Documentation to API Reference

To add a module to the API reference, edit `docs/source/api_reference.md`:

````markdown
## Your Module Name

```{eval-rst}
.. automodule:: your_module_name
   :members:
   :undoc-members:
   :show-inheritance:
```
````

### Current Documentation Structure

```
docs/source/
├── index.md                 # Main documentation page
├── getting_started.md       # User guide
├── configuration.md         # Config documentation
├── data_preparation.md      # Data prep guide
├── ui_customization.md      # UI customization
├── architecture.md          # System architecture
└── api_reference.md         # Auto-generated API docs
```

## Common Commands

```bash
# Build HTML documentation
cd docs && make html

# Clean build artifacts
cd docs && make clean

# Build and view in one command
cd docs && make html && open build/html/index.html

# Rebuild everything (useful after making changes)
cd docs && make clean && make html
```

## Troubleshooting

### Import Errors

If Sphinx can't find your modules, check that:
1. `sys.path.insert(0, os.path.abspath("../../scripts"))` is in `conf.py`
2. Your modules are importable from the Python environment

### Missing Documentation

If your code isn't showing up:
1. Check that docstrings are present
2. Verify the module is added to `api_reference.md`
3. Try `make clean && make html` to rebuild from scratch

### Pydantic Models Not Rendering

Ensure:
1. `sphinxcontrib.autodoc_pydantic` is installed
2. It's listed in `extensions` in `conf.py`
3. Your models inherit from `pydantic.BaseModel`

## Best Practices

1. **Write clear docstrings**: Use Google or NumPy style for consistency
2. **Add Field descriptions**: Use `Field(description="...")` for Pydantic models
3. **Keep docs updated**: Rebuild after significant code changes
4. **Use type hints**: They're automatically included in the documentation
5. **Add examples**: Include usage examples in docstrings when helpful

## References

- [Sphinx Documentation](https://www.sphinx-doc.org/)
- [Pydantic Sphinx Extension](https://github.com/mansenfranzen/autodoc_pydantic)
- [MyST Parser (Markdown)](https://myst-parser.readthedocs.io/)
