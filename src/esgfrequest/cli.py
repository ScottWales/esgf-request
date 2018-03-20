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
from argparse import ArgumentParser
from itertools import islice
from distutils.util import strtobool
import esgf
import six
import sqlite3
import os
from datetime import datetime

text_facets = [
        'query',
        'title',
        'version',
        'checksum',
        'checksum_type',
        'start',
        'end',
        'cf_standard_name',
        'ensemble',
        'experiment',
        'institute',
        'cmor_table',
        'model',
        'project',
        'realm',
        'time_frequency',
        'variable',
        'variable_long_name',
        ]
bool_facets = {
        'replica': {
            'help': "Return only replicas (true), only orignals (false, default), or all (all)",
            'default': False,
            'nargs': '?',
            'const': True,
            },
        'latest': {
            'help': "Return only latest (true, default), only outdated (false), or all (all)",
            'default': True,
            'nargs': '?',
            'const': True,
            },
        'distrib': {
            'help': "Search all nodes (true, default), or just the specified server (false)",
            'default': True,
            'nargs': '?',
            'const': True,
            },
        }

def connect_db():
    conn = sqlite3.connect('checksums.db')
    return conn.cursor()

def search_for_matches(cursor, filename, checksum):
    partial = 0

    cursor.execute('SELECT count(md5) FROM checksums WHERE md5 = ? OR sha256 = ? GROUP BY md5',
                   (checksum, checksum))
    r = cursor.fetchone()
    if r is None:
        exact = 0
    else:
        exact = 1
        return (exact, partial)

    cursor.execute('SELECT count(filename) FROM checksums WHERE filename = ? GROUP BY filename',
                   (filename,))
    r = cursor.fetchone()
    if r is None:
        partial = 0
    else:
        partial = 1

    return (exact, partial)

def bool_or_all_arg(value):
    try:
        return strtobool(value)
    except ValueError:
        if value.lower() in ['a','all']:
            return None
        else:
            raise

def cli():
    parser = ArgumentParser()

    for f in text_facets:
        parser.add_argument('--%s'%f, action='append', nargs='+')

    for f, args in six.iteritems(bool_facets):
        parser.add_argument('--%s'%f, type=bool_or_all_arg, **args)

    parser.add_argument('--search_url',
            help="ESGF search API endpoint (e.g. https://esgf.nci.org.au/esg-search/search)",
            default="https://esgf.nci.org.au/esg-search/search"
            )
    parser.add_argument('--limit',
            help="Maximum number of files to search",
            type=int,
            default=1000)

    args = vars(parser.parse_args())

    limit = args.pop('limit')

    cursor = connect_db()

    results, count = search_esgf(args, limit, cursor)

    print_results(results, count, limit)

    make_request(results)

def search_esgf(args, limit, cursor):

    g = esgf.search_dataset_files_generator(fields=['dataset_id', 'variable', 'title', 'checksum', 'size'], **args)
    results = {}
    count = 0
    for doc in islice(g,limit):
        key = doc['dataset_id'] + ' ' + doc['variable'][0]
        r = results.get(key, {'matches':0,'misses':0, 'size':0, 'partial':0})

        exact, partial = search_for_matches(cursor, doc['title'], doc['checksum'][0])

        # NCI files are always local
        if doc['dataset_id'].endswith('esgf.nci.org.au'):
            exact, partial = 1, 0

        r['matches'] += exact
        r['misses'] += 1 - exact - partial
        r['partial'] += partial
        r['size'] += doc['size']
        r['dataset_id'] = doc['dataset_id']
        r['variable'] = doc['variable'][0]
        results[key] = r
        count += 1

    # Print a newline after the progress bar
    print()
    return results, count

def print_results(results, count, limit):

    print("local\tpartial\tmissing\t\tsize\tid")
    for k, v in six.iteritems(results):
        name = k
        if v['partial'] > 0:
            name = "\u001b[33m%s\u001b[39;49m"%k
        if v['misses'] > 0:
            name = "\u001b[31m%s\u001b[39;49m"%k
        print("% 5d\t% 7d\t% 7d\t%s\t%s"%(
            v['matches'], v['partial'], v['misses'], size_str(v['size']), name))

    if count == limit:
        print("Reached maximum file limit (%d), some matches may be missing"%limit)

    total_misses = sum([v['misses'] for v in six.itervalues(results)])
    missing_size = sum([v['size'] if v['misses'] > 0 else 0 for v in six.itervalues(results)])

    total_partial = sum([v['partial'] for v in six.itervalues(results)])
    partial_size = sum([v['size'] if v['partial'] > 0 else 0 for v in six.itervalues(results)])

    print()
    print("Total missing:   % 4d files, %s"%(
        total_misses, size_str(missing_size)))
    print("Partial matches: % 4d files, %s (e.g. different versions)"%(
        total_partial,
        size_str(partial_size))
        )

def make_request(results):
    total_misses = sum([v['misses'] for v in six.itervalues(results)])
    missing_size = sum([v['size'] if v['misses'] > 0 else 0 for v in six.itervalues(results)])

    total_partial = sum([v['partial'] for v in six.itervalues(results)])
    partial_size = sum([v['size'] if v['partial'] > 0 else 0 for v in six.itervalues(results)])

    if total_misses > 0:
        request_download = input_bool("\nSubmit a request for %s of missing data? (yes/[no]) "%(size_str(missing_size)))
        if request_download:
            to_download = [(v['dataset_id'], v['variable']) for v in six.itervalues(results) if v['misses'] > 0]
            render_request(to_download, prefix='request')
            print("\nRequest submitted")

    if total_partial > 0:
        request_update = input_bool("\nRequest updates for  %s of partial matches? (yes/[no]) "%(size_str(partial_size)))
        if request_update:
            to_download = [(v['dataset_id'], v['variable']) for v in six.itervalues(results) if v['partial'] > 0]
            render_request(to_download, prefix='update')
            print("\nRequest submitted")


def input_bool(prompt, default=False):
    r = input(prompt)
    try:
        return strtobool(r)
    except ValueError:
        return default


def request_missing(results, request_partial):
    to_download = [(v['dataset_id'], v['variable']) for v in six.itervalues(results) if v['misses'] > 0]
    render_request(to_download, prefix='request')

requestdir = '.'

def render_request(to_download, prefix):
    requestfile = os.path.join(requestdir, '_'.join([prefix, os.environ['USER'], datetime.now().strftime("%Y%m%dT%H%M") + '.txt']))
    with open(requestfile, 'w') as f:
        for val in to_download:
            docs = esgf.search_files_generator(dataset_id = val[0], variable= val[1], fields=['title', 'url', 'checksum_type', 'checksum'])
            for d in docs:
                url = [u[0] for u in [u.split('|') for u in d['url']] if u[2] == 'HTTPServer'][0]
                f.write("'%s' '%s' '%s' '%s'\n"%(d['title'], url, d['checksum_type'][0], d['checksum'][0]))


def size_str(size):
    from math import log, floor
    if size == 0:
        r = 0
    else:
        r = int(floor(log(size, 1000)))
    prefix = [' ', 'k', 'm', 'g', 'T']
    return "% 6.1f %sb" % (size / 1000**r, prefix[r])

def terminal_hyperlink(url, text):
    return u"\u001b]8;%s\u001b\\%s\u001b]8;;\u001b\\"%(url, text)

if __name__ == '__main__':
    cli()

