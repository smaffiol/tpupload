# test_tp_parser.py
# -*- coding: utf-8 -*-

import pytest
import os
from testfixtures import TempDirectory
import tpupload

raw_files = ['raw_1','raw_2','raw_4']

def prep(filter):
    found = []
    missing = []

    with TempDirectory() as d:
        source = d.path
        d.makedir('raw')
        with open(filter,'wb') as fd:
            for ftw in raw_files:
                d.write(os.path.join(d.path,
                                     'raw',
                                     ftw),
                        b'some foo thing')
                fd.write("{0}\n".format(ftw))
        try:
            found,missing = tpupload.get_files(d.path, filter)
        finally:
            os.remove(filter)
    return (found,missing)

def test_filters_ok():
    """
    Test parsing on filters
    """
    f,m = prep('/tmp/method__raw.txt')
    assert len(f) > 0 and len(m) == 0

def test_filter_file_ok():
    """
    Test parsing on filters
    """
    f,m = prep('/tmp/method__raw_files.txt')
    assert len(f) > 0 and len(m) == 0

def test_filters_failed_parse():
    """
    Test parsing on filters
    """
    with pytest.raises(Exception) as e_info:
        f,m = prep('/tmp/method_raw.txt')



            


