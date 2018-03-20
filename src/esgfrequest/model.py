#!/usr/bin/env python
# Copyright 2018 ARC Centre of Excellence for Climate Extremes
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

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Text
from sqlalchemy.dialects import postgresql

Base = declarative_base()

uuid_t = Text().with_variant(postgresql.UUID, 'postgresql')


class Checksum(Base):
    __tablename__ = 'checksums'
    id = Column('ch_hash', uuid_t, primary_key=True)
    md5 = Column('ch_md5', Text)
    sha256 = Column('ch_sha256', Text)

   
class Basename(Base):
    __tablename__ = 'basenames'
    id = Column('pa_hash', uuid_t, primary_key=True)
    basename = Column(Text)
