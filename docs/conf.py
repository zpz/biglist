# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html


# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'biglist'
copyright = '2022, Zepu Zhang'
author = 'Zepu Zhang'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration


extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.todo',
    'sphinx.ext.viewcode',
    ]

autodoc_default_options = {
    'members': True,
    'undoc-members': True,
    'special-members': '__init__, __getitem__, __iter__, __next__, __len__',
    'member-order': 'bysource',
    'show-inheritance': True,
}


templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# Interesting styles to consider:
#  toc panel on left
#   bizstyle
#   pyramid
#   nature
#  toc panel on right
#   sphinxdoc
#   furo
#  no toc panel
#   scrolls  (good for very small, single-page doc)
html_theme = 'furo'


html_static_path = ['_static']
