#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

PYTHONPATH=. python /vagrant/bin/luigi/bin/luigid --background --logdir /tmp
python run.py MultimediaTask --local-scheduler --date 2014-01-23
python run.py SitesTask --local-scheduler --date 2014-01-23
python run.py CatalogueTask --local-scheduler --date 2014-01-23 --force
"""

import luigi
import ke2sql.tasks

if __name__ == "__main__":
    luigi.run()