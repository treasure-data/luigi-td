# Luigi-TD

[![Build Status](https://travis-ci.org/treasure-data/luigi-td.svg?branch=master)](https://travis-ci.org/treasure-data/luigi-td)
[![Code Health](https://landscape.io/github/treasure-data/luigi-td/master/landscape.svg?style=flat)](https://landscape.io/github/treasure-data/luigi-td/master)
[![Coverage Status](https://coveralls.io/repos/treasure-data/luigi-td/badge.svg?branch=master)](https://coveralls.io/r/treasure-data/luigi-td?branch=master)
[![PyPI version](https://badge.fury.io/py/luigi-td.svg)](http://badge.fury.io/py/luigi-td)
[![Documentation Status](https://readthedocs.org/projects/luigi-td/badge/?version=latest)](https://readthedocs.org/projects/luigi-td/?badge=latest)

## Install

You can install the releases from [PyPI](https://pypi.python.org/).

```sh
$ pip install luigi-td
```

## Examples

```python
# tasks.py
import luigi
import luigi_td

class MyQuery(luigi_td.Query):
    type = 'presto'
    database = 'sample_datasets'

    def query(self):
        return "SELECT count(1) FROM www_access"

if __name__ == '__main__':
    luigi.run()
```

```sh
$ export TD_API_KEY="YOUR-API-KEY"
$ python tasks.py MyQuery --local-scheduler
```

## Documentation

* [Getting Started](http://luigi-td.readthedocs.org/en/latest/gettingstarted.html)

## License

Apache Software License, Version 2.0
