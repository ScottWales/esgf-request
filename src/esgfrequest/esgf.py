#!/usr/bin/env python
# Copyright 2018 Scott Wales
# author: Scott Wales <scott.wales@unimelb.edu.au>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import print_function
import requests
import six
from . import logger

def search_raw(
        search_url='https://esgf.nci.org.au/esg-search/search',
        distrib=True,
        replica=None,
        latest=None,
        limit=10,
        offset=0,
        sort=None,
        query='*',
        type='Dataset',
        **kwargs
        ):
    
    params = {
            'distrib': distrib,
            'replica': replica,
            'latest': latest,
            'limit': limit,
            'offset': offset,
            'sort': sort,
            'query': query,
            'type': type,
            'format': 'application/solr+json',
            }

    for key, value in six.iteritems(kwargs):
        if isinstance(value, six.string_types):
            params[key] = value
        else:
            try:
                params[key] = ','.join(value)
            except TypeError:
                params[key] = value

    r = requests.get(search_url, params=params, timeout=30)

    logger.info("GET %s"%r.url)

    r.raise_for_status()

    return r.json()


def search_datasets(**kwargs):
    return search_raw(**kwargs)

def search_files(fields=None, **kwargs):
    return search_raw(type='File', **kwargs)

def search_dataset_files(**kwargs):
    """
    Returns files that match given dataset constraints
    """
    datasets = search_datasets(fields='id', **kwargs)

    ids = [d['id'] for d in datasets['response']['docs']]
    return search_files(dataset_id = ids, fields=fields)

def search_datasets_generator(**kwargs):
    """
    Returns a geneartor producing matching datasets
    """
    offset = 0
    limit = kwargs.pop('limit', 100)

    while True:
        r = search_datasets(offset=offset, limit=limit, **kwargs)
        print('.',end='',flush=True)

        for doc in r['response']['docs']:
            yield doc

        offset += limit
        if r['response']['numFound'] < offset:
            break


def search_files_generator(**kwargs):
    """
    Returns a geneartor producing matching files
    """
    offset = 0
    limit = kwargs.pop('limit', 100)

    while True:
        r = search_files(offset=offset, limit=limit, **kwargs)
        print('.',end='',flush=True)

        for doc in r['response']['docs']:
            yield doc

        offset += limit
        if r['response']['numFound'] < offset:
            break


def search_dataset_files_generator(**kwargs):
    """
    Returns a generator producing files that are part of matching
    datasets

    This is primarily to allow searching for specific versions, as version
    information is kept with the Dataset, not the File

    Basically:

        r = search(type=Dataset, fields='id', **kwargs)
        for doc in r:
            search(type=File, dataset_id=doc['id'],
                   fields=kwargs['fields'],
                   variable=kwargs['variable'])

    So this is primarily a datset search, but variable-specific facets and the
    field list are passed through to the file search
    """

    offset = 0
    limit = kwargs.pop('limit', 10)
    fields = kwargs.pop('fields', None)

    while True:
        r = search_datasets(offset=offset, limit=limit, fields='id', **kwargs)
        print('.',end='',flush=True)

        ids = [d['id'] for d in r['response']['docs']]

        for f in search_files_generator(dataset_id = ids, fields=fields,
                    variable=kwargs.get('variable'),
                    cf_standard_name=kwargs.get('cf_standard_name'),
                    variable_long_name=kwargs.get('variable_long_name'),
                    distrib=kwargs.get('distrib'),
                    ):
            yield f

        offset += limit
        if r['response']['numFound'] < offset:
            break
