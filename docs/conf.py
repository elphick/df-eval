# Configuration file for the Sphinx documentation builder.

import os
import sys

# Add the src directory to the path
sys.path.insert(0, os.path.abspath("../src"))

# -- Project information -----------------------------------------------------
project = "df-eval"
copyright = "2026, Greg Elphick"
author = "Greg Elphick"
release = "0.1.0"

# -- General configuration ---------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx_gallery.gen_gallery",
    "sphinx_autodoc_typehints",
    "myst_parser",  # Keep for including README.md
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# Source file suffixes
source_suffix = {
    '.rst': 'restructuredtext',
    '.md': 'markdown',
}

# -- Options for HTML output -------------------------------------------------
html_theme = "sphinx_book_theme"
html_static_path = ["_static"]
html_title = "df-eval Documentation"

# Use project branding (icons) for the docs
html_logo = "_static/branding/df-eval-icon.svg"
html_favicon = "_static/branding/df-eval-icon.svg"

html_theme_options = {
    "repository_url": "https://github.com/elphick/df-eval",
    "use_repository_button": True,
    "use_issues_button": True,
    "use_edit_page_button": True,
    "path_to_docs": "docs",
    "repository_branch": "main",
    "logo": {
        "image_light": "_static/branding/df-eval-icon-light.svg",
        "image_dark": "_static/branding/df-eval-icon-dark.svg",
        "text": f"df-eval<br>({release})",  # shows version in the top-left

    },
}


# -- Extension configuration -------------------------------------------------

# Napoleon settings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True

# Autodoc settings
autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "special-members": "__init__",
    "undoc-members": True,
    "exclude-members": "__weakref__",
}

# Intersphinx configuration
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/docs/", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
}

# MyST parser configuration
myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "substitution",
]

# Sphinx Gallery configuration
sphinx_gallery_conf = {
    "examples_dirs": ["examples"],
    "gallery_dirs": ["auto_examples"],
    "filename_pattern": r".*\.py",
}

