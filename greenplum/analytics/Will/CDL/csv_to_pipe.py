# -*- coding: utf-8 -*-
"""
Created on Sun Jul 10 14:13:35 2022

@author: wcoughlin
"""

import csv

work_path = 'S:\\APCD\\TestData\\'
workfile = work_path + 'data-test_small.csv'

pipe_file = work_path + 'data-test-small.txt'

with open(workfile) as fin:
    # newline='' prevents extra newlines when using Python 3 on Windows
    # https://stackoverflow.com/a/3348664/3357935
    with open(pipe_file, 'w', newline='') as fout:
        reader = csv.DictReader(fin, delimiter=',')
        writer = csv.DictWriter(fout, reader.fieldnames, delimiter='|')
        writer.writeheader()
        writer.writerows(reader)