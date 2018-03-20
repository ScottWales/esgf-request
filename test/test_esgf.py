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
from esgf import *

def test_search_raw():
    r = search_raw(fields='id')
    assert r['response']['numFound'] > 0

def test_search_dataset():
    r = search_datasets(
            fields='id',
            model='CMCC-CMS',
            experiment='rcp45',
            ensemble='r1i1p1',
            cmor_table='Amon',
            version='20170725'
            )
    assert r['response']['numFound'] == 1

def test_search_dataset_files():
    r = search_dataset_files(
            fields='id',
            model='CMCC-CMS',
            experiment='rcp45',
            ensemble='r1i1p1',
            cmor_table='Amon',
            version='20170725',
            variable='tas',
            )
    assert r['response']['numFound'] == 1

