#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
from crontab import CronTab

data_file = sys.argv[1] if len(sys.argv) > 1 else None
hosts = sys.argv[2:] if len(sys.argv) > 2 else None
cron = CronTab(user='wzx')
jar = 'LabelLogAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar'

# 数据分发
os.system(f'python script/create_dataset.py {data_file} {len(hosts) + 1}')
for index, host in enumerate(hosts + ['localhost']):
    os.system(f'scp tmp_{index} {host}:log')
    print(f'tmp_{index} send to {host}:log')
    os.remove(f'tmp_{index}')

# 编译上传jar包到HDFS
ret = os.system(f'mvn package')
if ret is not 0:
    raise RuntimeError("maven build error")
os.system(f'hdfs dfs -put target/{jar}')
print('build and upload package')

# 创建topic
# todo 先删除再创建
os.system(f'python shell/topic.py create {" ".join(hosts)}')
print('create kafka topic')

# 创建数据表
os.system(f'impala-shell -f sql/impala.sql --var=cur_month="$(date +"%Y-%m-01 00:00:00")"')
print('create database table')

# 离线sql作业，三个月一次
cron.remove_all(comment='sql')
cron.write()
job = cron.new(
    command='impala-shell -f sql/window_data_move.sql '
            '--var=cur_month="$(date +"%Y-%m-01 00:00:00")" '
            '&>> log/data-move.log',
    comment='sql')
job.setall('0 1 1 * *')
job = cron.new(
    command='impala-shell -f sql/window_partition_shift.sql '
            '--var=cur_month="$(date +"%Y-%m-01 00:00:00")" '
            '&>> log/data-move.log',
    comment='sql')
job.setall('30 1 1 * *')
job = cron.new(
    command='impala-shell -f sql/window_view_alter.sql '
            '--var=cur_month="$(date +"%Y-%m-01 00:00:00")" '
            '&>> log/view-alter.log''',
    comment='sql')
job.setall('0 2 1 * *')
cron.write()
print('create crontab job to sql')

# 提交实时Flink作业
os.system('python shell/submit.py flink')
print('submit flink job')

# 离线spark作业
cron.remove_all(comment='spark')
cron.write()
# todo 两个job
job = cron.new(command='python shell/submit.py spark &>> log/spark-submit.log', comment='spark')
job.setall('0 0 * * *')
job = cron.new(command='python shell/submit.py spark &>> log/spark-submit.log', comment='spark')
job.setall('0 30 * * *')
cron.write()
print('create crontab job to submit spark job')

# mock kafka
for index, host in enumerate(hosts + ['localhost']):
    os.system(f'ssh {host} "hdfs dfs -get {jar}"')
    os.system('nohup java -classpath {jar} com.wzx.mock.Upload2Kafka &')
    print(f'mock kafka uploader on {host}')

