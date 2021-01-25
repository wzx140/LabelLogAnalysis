#!/usr/bin/env python
# -*- coding:utf-8 -*-

import sys
import datetime
from dateutil.relativedelta import relativedelta

data_file = sys.argv[1]
num = int(sys.argv[2])

line_num = sum(1 for _ in open(data_file, 'r'))
print(f"{data_file} has {line_num} lines")
split_line_num = line_num // num
# 数据切分
with open(data_file, 'r') as source:
    for index in range(num):
        date = datetime.date(datetime.date.today().year, datetime.date.today().month, 1)
        # 修改时间
        seconds = 3 * 30 * 24 * 60 * 60
        record_per_sec = split_line_num // seconds if split_line_num // seconds > 1 else 1
        cur_record_per_sec = 0
        with open(f'tmp_{index}', 'w') as target:
            for _ in range(split_line_num):
                if cur_record_per_sec == record_per_sec:
                    date += relativedelta(seconds=1)
                    cur_record_per_sec = 0
                date_splits = date.strftime('%Y-%m-%d %H:%M:%S').split(' ')
                line_splits = source.readline().split(' ')
                line_splits[3], line_splits[4] = '[' + date_splits[0], date_splits[1] + ']'
                target.write(' '.join(line_splits))
                cur_record_per_sec += 1
            print(f'generate dataset tmp_{index}')
