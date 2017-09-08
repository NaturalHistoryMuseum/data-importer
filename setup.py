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
    name='data-importer',
    version=__version__,
    description='Data Portal import pipeline',
    author='Ben Scott',
    author_email='ben@benscott.co.uk',
    url='https://github.com/NaturalHistoryMuseum/data-importer',
    license='Apache License 2.0',
    packages=[
        'data_importer',
    ],
)



