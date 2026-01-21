# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(".."))

# Project information
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "LISA Artifacts"
author = "LISA Consortium"

# The full version, including alpha/beta/rc tags
release = "latest"  # Will be overridden by multiversion

# General configuration
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "sphinx_multiversion",
    "sphinxcontrib.autodoc_pydantic",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
]

exclude_patterns = ["_build"]

# Options for MyST
myst_enable_extensions = [
    "colon_fence",
    "deflist",
]

# Options for HTML output
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_show_sphinx = False

html_static_path = ["_static"]

templates_path = [
    "_templates",
]

# These paths are either relative to html_static_path
# or fully qualified paths (eg. https://...)
html_css_files = [
    "css/custom.css",
]


# Multiversion configuration
# Build versions from tags matching vX.Y.Z and main branch
# Whitelist pattern for tags (set to None to ignore all tags)
smv_tag_whitelist = r"^v.*$"

# Whitelist pattern for branches (set to None to ignore all branches)
smv_branch_whitelist = r"^.*$"

# Whitelist pattern for remotes (set to None to use local branches only)
smv_remote_whitelist = None

# Pattern for released versions
smv_released_pattern = r"^tags/v.*$"

# Format for versioned output directories inside the build directory
smv_outputdir_format = "{ref.name}"

# Determines whether remote or local git branches/tags are preferred if their output
# dirs conflict
smv_prefer_remote_refs = False
