.. carcosa documentation master file, created by
   sphinx-quickstart on Wed Dec 19 16:12:24 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to carcosa's documentation!
===================================

Carcosa is a python library to programmatically control remote clusters.
It's highly inspired by `fyrd`_.

The main problem with fyrd is that's not designed to control remote clusters
and have a lot of functionalities, giving less control to the programmer about
how the submitted jobs are executed.

Carcosa wants to be simple, having a clean, high level interface to execute and
control jobs. It does not manage dependency between jobs or other fancy stuff,
like executing pandas dataframes actions or having a local queue system.

.. csv-table:: Fyrd vs Carcosa
	:header: "Feature", "carcosa", "fyrd"
	:widths: 50, 10, 10

	"Remote cluster job management", "yes", "no"
	"Metrics for a job", "yes", "no"
	"Modular", "yes", "\*"
	"Python function in queue systems", "yes", "yes"
	"Command execution in queue systems", "yes", "yes"
	"Profiles", "no", "yes"
	"File submission", "no", "yes"
	"Builtin local queue system", "no", "yes"

\* It's easy to add a batch system in a clean way, it's not easy to add new
functionalities while maintaining the code clean.

.. _fyrd: https://github.com/MikeDacre/fyrd

Installation
============

Carcosa is not yet in PyPI (it'll be at some point). It can be installed with
pip.

Python version 3.6 or greater is needed. Using a virtual environment is
recommended.

.. code-block:: bash

	$ python3 --version # Make sure >python3.6 is installed
	Python 3.6.x
	$ git clone https://github.com/quim0/carcosa && cd carcosa
	$ pip install --user -e .       # To install just carcosa
	$ pip install --user -e .[test] # To install carcosa with testing
	$ pip install --user -e .[dev]  # To install carcosa with testing and docs

.. toctree::
   :maxdepth: 3
   :caption: Contents

   examples
   api

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
