#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
from pyhocon import ConfigFactory

command = sys.argv[1]
conf = ConfigFactory.parse_file('application.conf')
zookeeper_list = conf['wzx.zoo.servers']
broker_list = conf['wzx.topic.weblogs.brokers']
topic_name = conf['wzx.topic.weblogs.name']

if command == 'describe':
    os.system(f"kafka-topics --zookeeper {zookeeper_list} --describe")
elif command == 'create':
    os.system(
        f"kafka-topics --create --zookeeper {zookeeper_list} --replication-factor 1 --partitions 2 --topic {topic_name}")
elif command == 'delete':
    os.system(f"kafka-topics --zookeeper {zookeeper_list} --delete --topic {topic_name}")
    os.system(f'zookeeper-client -server {zookeeper_list} deleteall /brokers/topics/{topic_name}')
elif command == 'console':
    os.system(f"kafka-console-consumer --bootstrap-server {broker_list} --topic {topic_name}")
