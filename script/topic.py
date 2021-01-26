#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
from functools import reduce

command = sys.argv[1] if len(sys.argv) > 1 else None
hosts = sys.argv[2:] if len(sys.argv) > 2 else None
zookeeper_list = reduce(lambda s1, s2: s1 + s2, map(lambda s: s + ':2181,', hosts))[:-1]
broker_list = reduce(lambda s1, s2: s1 + s2, map(lambda s: s + ':9092,', hosts))[:-1]

if command == 'describe':
    os.system(f"kafka-topics --zookeeper {zookeeper_list} --describe")
elif command == 'create':
    os.system(
        f"kafka-topics --create --zookeeper {zookeeper_list} --replication-factor 1 --partitions 2 --topic weblogs")
elif command == 'delete':
    os.system(f"kafka-topics --zookeeper {zookeeper_list} --delete --topic weblogs")
    os.system(f'zookeeper-client -server {zookeeper_list} deleteall /brokers/topics/weblogs')
elif command == 'console':
    os.system(f"kafka-console-consumer --bootstrap-server {broker_list} --topic weblogs")
