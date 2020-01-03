#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-slack',
      version='0.0.1',
      description='Singer.io tap for extracting data from the Slack Web API',
      author='dwallace@envoy.com',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_slack'],
      install_requires=[
          'singer-python==5.9.0',
          'slackclient==2.0.1'
      ],
      python_requires='>=3.6',
      entry_points='''
          [console_scripts]
          tap-slack=tap_slack:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_slack': [
              'schemas/*.json'
          ]
      })
