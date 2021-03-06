# Carcosa

[![Build Status](https://travis-ci.com/quim0/carcosa.svg?branch=master)](https://travis-ci.com/quim0/carcosa)

Carcosa is a library to programmatically control remote clusters using python.
It's highly inspired by [fyrd](https://github.com/MikeDacre/fyrd), some parts
are even the same.

# Carcosa vs fyrd

* Carcosa is designed to operate with *remote* queue systems, fyrd expects the
  queue system to be available at localhost.
* Carcosa code is simpler, it aims to be lightweight and give more control to
  the programmer. It have also less features (no pandas stuff, local queues, no
  job dependencies managed by carcosa...).
* Carcosa gets metrics for the jobs.

Know what your needs are and then decide what to use.

# Install

``` bash
$ git clone https://github.com/quim0/carcosa
$ cd carcosa
$ pip install -e . # Use --user to avoid system wide installation
```

# Getting started

TODO...

# Docs

TODO...

# Test

To run the tests, install carcosa with:

``` bash
pip install -e .[test]  # Use --user if not in a venv
```

Then run the tests:

``` bash
cd tests && pytest
```

# Name

Carcosa is a city, mysterious, ancient, and possibly cursed. Cluster management
systems are also mysterious, ancient and possibly cursed.
