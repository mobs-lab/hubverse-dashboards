import os
import sys

# -- Path setup --------------------------------------------------------------
# Add the scripts directory to sys.path so Sphinx autodoc can import the modules.
sys.path.insert(0, os.path.abspath("../../scripts"))

# -- Project information -----------------------------------------------------
project = "Hubverse Dashboard"
copyright = "2026, MOBs-Lab"
author = "MOBs-Lab"
release = "1.0"

# -- General configuration ---------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",              # Generate docs from docstrings
    "sphinx.ext.napoleon",             # Google/NumPy style docstring support
    "sphinx.ext.viewcode",             # Link to highlighted source code
    "sphinx.ext.intersphinx",          # Cross-reference external projects
    "sphinxcontrib.autodoc_pydantic",  # Pydantic model documentation
    "myst_parser",                     # Markdown support (MyST)
    "sphinxcontrib.mermaid",           # Mermaid diagram support
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = []

# -- Autodoc configuration ---------------------------------------------------
autodoc_default_options = {
    "member-order": "bysource",
    "undoc-members": True,
    "show-inheritance": True,
}

# -- Options for Pydantic autodoc --------------------------------------------
autodoc_pydantic_model_show_json = True
autodoc_pydantic_settings_show_json = False
autodoc_pydantic_field_list_validators = False
autodoc_pydantic_model_member_order = "bysource"
autodoc_pydantic_model_show_config_summary = False
autodoc_pydantic_model_show_field_summary = True

# -- Napoleon configuration --------------------------------------------------
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False
napoleon_use_param = True
napoleon_use_rtype = True

# -- Options for HTML output -------------------------------------------------
html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

html_theme_options = {
    "navigation_depth": 3,
    "collapse_navigation": False,
    "sticky_navigation": True,
}

# -- Intersphinx mapping -----------------------------------------------------
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
    "pydantic": ("https://docs.pydantic.dev/latest/", None),
}

# -- MyST Parser configuration -----------------------------------------------
myst_enable_extensions = [
    "colon_fence",
    "admonition",
    "deflist",
    "fieldlist",
    "tasklist",
]

# Auto-generate heading anchors for H1 through H3
myst_heading_anchors = 3

# -- Mermaid configuration ---------------------------------------------------
mermaid_version = "10.6.1"
