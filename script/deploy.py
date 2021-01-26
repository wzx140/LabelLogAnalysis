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
os.system(f'python3 script/create_dataset.py {data_file} {len(hosts) + 1}')
for index, host in enumerate(hosts + ['localhost']):
    print(f'tmp_{index} send to {host}:data/weblogs')
    os.system(f'ssh {host} "mkdir data"')
    os.system(f'scp tmp_{index} {host}:data/weblogs')
    os.remove(f'tmp_{index}')

# 编译上传jar包到HDFS
print('build and upload package')
os.system('mvn install:install-file '
          '-Dfile=lib/com.ggstar/ipdatabase/1.0-SNAPSHOT/ipdatabase-1.0-SNAPSHOT.jar '
          '-DgroupId=com.ggstar -DartifactId=ipdatabase -Dversion=1.0-SNAPSHOT -Dpackaging=jar')
ret = os.system('mvn -DskipTests package')
if ret != 0:
    raise RuntimeError("maven build error")
os.system(f'hdfs dfs -put target/{jar}')

# 创建topic
print('create kafka topic')
os.system(f'python3 script/topic.py delete {" ".join(hosts)}')
os.system(f'python3 script/topic.py create {" ".join(hosts)}')

# 创建数据表
print('create database table')
os.system('impala-shell -f sql/impala.sql --var=cur_month="$(date +"%Y-%m-01 00:00:00")"')

# 离线sql作业，三个月一次
print('create crontab job to sql')
os.system('mkdir -p cd /home/wzx/LabelLogAnalysis/log')
cron.remove_all(comment='sql')
cron.write()
job = cron.new(
    command='cd /home/wzx/LabelLogAnalysis;'
            'impala-shell -f sql/window_data_move.sql '
            '--var=cur_month="$(date +"%Y-%m-01 00:00:00")" '
            '&>> log/data-move.log',
    comment='sql')
job.setall('0 1 1 * *')
job = cron.new(
    command='cd /home/wzx/LabelLogAnalysis;'
            'impala-shell -f sql/window_partition_shift.sql '
            '--var=cur_month="$(date +"%Y-%m-01 00:00:00")" '
            '&>> log/data-move.log',
    comment='sql')
job.setall('30 1 1 * *')
job = cron.new(
    command='cd /home/wzx/LabelLogAnalysis;'
            'impala-shell -f sql/window_view_alter.sql '
            '--var=cur_month="$(date +"%Y-%m-01 00:00:00")" '
            '&>> log/view-alter.log''',
    comment='sql')
job.setall('0 2 1 * *')
cron.write()

# 提交实时Flink作业
print('submit flink job')
os.system('python3 script/submit.py flink')

# 离线spark作业
print('create crontab job to submit spark job')
cron.remove_all(comment='spark')
cron.write()
job = cron.new(command='cd /home/wzx/LabelLogAnalysis;'
                       'python3 script/submit.py spark_video_visit_over_100 '
                       '&>> log/spark-submit.log',
               comment='spark')
job.setall('0 0 * * *')
job = cron.new(command='cd /home/wzx/LabelLogAnalysis;'
                       'python3 script/submit.py spark_new_register '
                       '&>> log/spark-submit.log',
               comment='spark')
job.setall('30 0 * * *')
cron.write()

# mock kafka
for index, host in enumerate(hosts + ['localhost']):
    os.system(f'ssh {host} "cd data;hdfs dfs -get {jar}"')
    print(f'mock kafka uploader on {host}')
    os.system(f'ssh {host} "cd data;nohup java -classpath {jar} com.wzx.mock.Upload2Kafka &"')
