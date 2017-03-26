#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""
try:
    from setuptools import setup
except:
    from distutils.core import setup


__version__ = '1.0'

setup(
    name='ke2sql',
    version=__version__,
    description='Import KE Data into postgres',
    author='Ben Scott',
    author_email='ben@benscott.co.uk',
    url='https://github.com/NaturalHistoryMuseum/ke2sql',
    license='Apache License 2.0',
    packages=[
        'ke2sql',
    ],
    entry_points='''
        [console_scripts]
        init-ke2sql=ke2sql.cli:init
    ''',
)



