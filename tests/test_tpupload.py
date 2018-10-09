# test_tp_parser.py
# -*- coding: utf-8 -*-

import pytest
import os
from testfixtures import TempDirectory
import tpupload
import sh
import tempfile
import shutil

raw_files = ['raw_1','raw_2','raw_4']
raw_files_ok = ['raw_1','raw_2','raw_4']
raw_files_failed = ['raw_1','raw_2','raw_3']

source = tempfile.mkdtemp()

def prep_filter(filter,data_list):
    with open(filter,'wb') as fd:
        for data in data_list:
            fd.write("{0}\n".format(data))


def prep_data_folder(td,data_list):
    if not os.path.isdir(os.path.join(td,'raw')):
        os.mkdir(os.path.join(td,'raw'))
    for ftw in data_list:
        sh.touch(os.path.join(td,
                              'raw',
                              ftw))

def prep(filter):
    found = []
    missing = []

    with TempDirectory() as d:
        source = d.path
        d.makedir('raw')
        for ftw in raw_files:
            d.write(os.path.join(d.path,
                                 'raw',
                                 ftw),
                    b'some foo thing')
        try:
            found,missing = tpupload.get_files(d.path, filter)
        finally:
            os.remove(filter)
    return (found,missing)

def test_parse_filters_ok():
    """
    Test parsing on filters
    """
    filter = '/tmp/method__raw.txt'
    prep_filter(filter,raw_files_ok)
    prep_data_folder(source,raw_files)
    try:
        found,missing = tpupload.get_files(source, filter)
    finally:
        os.remove(filter)
    # assert len(f) > 0 and len(m) == 0

def test_parse_filter_file_ok():
    """
    Test parsing on filters
    """
    filter = '/tmp/method__raw_files.txt'
    prep_filter(filter,raw_files_ok)
    prep_data_folder(source,raw_files)
    try:
        found,missing = tpupload.get_files(source, filter)
    finally:
        os.remove(filter)

def test_parse_filters_failed():
    """
    Test parsing on filters
    """
    filter = '/tmp/method__raw-files.txt'
    prep_filter(filter,raw_files_ok)
    prep_data_folder(source,raw_files)
    try:
        found,missing = tpupload.get_files(source, filter)
    finally:
        os.remove(filter)

def test_get_files_ok():
    """
    Should fail as 
    """    
    filter = '/tmp/method__raw.txt'
    prep_data_folder(source,raw_files)
    prep_filter(filter,raw_files_ok)
    try:
        found,missing = tpupload.get_files(source, filter)
        assert len(found) > 0 and len(missing) == 0
    finally:
        os.remove(filter)

def test_get_files_failed():
    """
    Should fail as 
    """    
    filter = '/tmp/method__raw.txt'
    prep_data_folder(source,raw_files)
    prep_filter(filter,raw_files_failed)
    try:
        found,missing = tpupload.get_files(source, filter)
        assert len(found) > 0 and len(missing) == 0
    finally:
        os.remove(filter)
        shutil.rmtree(source)
