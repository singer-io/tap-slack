#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-slack',
      version='1.2.1',
      description='Singer.io tap for extracting data from the Slack Web API',
      author='dwallace@envoy.com',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_slack'],
      install_requires=[
          'singer-python==6.8.0',
          'slack-sdk==3.43.0',
      ],
      extras_require={
          'dev': [
            'pylint',
            'ipdb',
            'nose',
            'pytest',
            'pytest-cov',
            'coverage',
          ]
      },
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
