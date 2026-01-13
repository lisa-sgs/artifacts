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

# These paths are either relative to html_static_path
# or fully qualified paths (eg. https://...)
html_css_files = [
    "css/custom.css",
]


# Multiversion configuration
# Build versions from tags matching vX.Y.Z and main branch
smv_tag_whitelist = None
smv_branch_whitelist = None
smv_remote_whitelist = None
smv_outputdir_format = "{ref.name}"
smv_prefer_remote_refs = False
