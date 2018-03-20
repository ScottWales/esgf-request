#!/usr/bin/env python
from __future__ import print_function
from setuptools import setup, find_packages
import versioneer

setup(
        name='metadb',
        packages=find_packages('src'),
        package_dir={'': 'src'},
        version=versioneer.get_version(),
        cmdclass=versioneer.get_cmdclass(),

        install_requires=[
            'sqlalchemy',
            'netcdf4',
            'flask',
            ],
        entry_points={
            'console_scripts': [
                'metadb = metadb.cli:cli',
                ]}
        )
