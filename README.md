<!--header-start-->

# Data Importer

[![Tests](https://img.shields.io/github/actions/workflow/status/NaturalHistoryMuseum/data-importer/main.yml?branch=main&style=flat-square)](https://github.com/NaturalHistoryMuseum/data-importer/actions/workflows/main.yml)
[![Coveralls](https://img.shields.io/coveralls/github/NaturalHistoryMuseum/data-importer/main?style=flat-square)](https://coveralls.io/github/NaturalHistoryMuseum/data-importer)
[![Python version](https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11-blue?style=flat-square)](https://www.python.org/downloads)
[![Docs](https://img.shields.io/readthedocs/data-importer?style=flat-square)](https://data-importer.readthedocs.io)

<!--header-end-->

## Overview

<!--overview-start-->
This repository contains the pipeline which moves data from the Museum's EMu instance
into the [Data Portal](https://data.nhm.ac.uk).

Data is exported from EMu as texexport reports 5 nights a week and then this pipelines
moves it into the Data Portal's databases.

The data is versioned using the [Splitgill](https://pypi.org/project/splitgill/)
library and stored in MongoDB and Elasticsearch.

This is the 3rd iteration of this importer to date.
The code history in this repository only contains the first iteration of the importer as
the second iteration was developed in a private repository.
To make the code available for the current version, the private parts of the repository
have been moved to a different private repo, allowing this repository to be public.

<!--overview-end-->

## Installation

<!--installation-start-->
The data importer can be installed from GitHub:

```shell
pip install git+git://github.com/NaturalHistoryMuseum/data-importer.git#egg=data-importer
```

<!--installation-end-->

## Tests

<!--tests-start-->
Tests are run through docker-compose so that MongoDB and Elasticsearch are available for
real testing.

To run the tests:

```bash
docker compose run --build test
```

<!--tests-end-->
