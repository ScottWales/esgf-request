#!/usr/bin/env python
from __future__ import print_function
from setuptools import setup, find_packages
import versioneer

setup(
        name='esgfrequest',
        packages=find_packages('src'),
        package_dir={'': 'src'},
        version=versioneer.get_version(),
        cmdclass=versioneer.get_cmdclass(),

        install_requires=[
            'sqlalchemy',
            'netcdf4',
            'psycopg2',
            ],
        entry_points={
            'console_scripts': [
                'esgfrequest = esgfrequest.cli:cli',
                ]}
        )
