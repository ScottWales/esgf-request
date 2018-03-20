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

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import make_url
from getpass import getpass

Session = sessionmaker()


def connect(url, user=None, debug=False, init=False, session=Session):
    """
    Returns a sqlalchemy.Connection
    """
    _url = make_url(url)

    if user is not None:
        _url.username = user
        _url.password = getpass("Password for %s: "%user)

    engine = create_engine(_url, echo=debug)

    if init:
        from .model import Base
        Base.metadata.create_all(engine)

    session.configure(bind=engine)

    return engine.connect()
