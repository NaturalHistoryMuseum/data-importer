#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

PYTHONPATH=. python /vagrant/bin/luigi/bin/luigid --background --logdir /tmp
python run.py MultimediaTask --local-scheduler --date 2014-01-23

"""

import luigi
import ke2psql.tasks

if __name__ == "__main__":
    luigi.run()