from distutils.core import setup

setup(
    name='keboola-sandboxes-notebook-utils',
    version='1.3.1',
    url='https://github.com/keboola/sandboxes-notebook-utils',
    packages=['keboola_notebook_utils'],
    package_dir={'keboola_notebook_utils': ''},
    requires=['pip']
)
