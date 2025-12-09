import os
import sys

# -- Path setup --------------------------------------------------------------
# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here.
sys.path.insert(0, os.path.abspath("../../scripts"))

# -- Project information -----------------------------------------------------
project = "Hubverse Dashboard"
copyright = "2025, MOBs-Lab"
author = "MOBs-Lab"
release = "1.0"

# -- General configuration ---------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",  # Core library for generating docs from strings
    "sphinx.ext.napoleon",  # Support for Google/NumPy style docstrings
    "sphinx.ext.viewcode",  # Add links to highlighted source code
    "sphinxcontrib.autodoc_pydantic", # Pydantic Support
    "myst_parser",  # Markdown support
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = []

# -- Options for Pydantic output ---------------------------------------------
autodoc_pydantic_model_show_json = True
autodoc_pydantic_settings_show_json = False
autodoc_pydantic_field_list_validators = False
autodoc_pydantic_model_member_order = "bysource"

# -- Options for HTML output -------------------------------------------------
html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

# -- MyST Parser configuration -----------------------------------------------
# Enable toctree directive in markdown
myst_enable_extensions = [
    "colon_fence",
    "admonition",  # Enable 'Tip', 'Note', 'Warning' blocks
    "deflist",     # Definition lists
]

# Auto-generate heading anchors for H2 and H3
myst_heading_anchors = 3
