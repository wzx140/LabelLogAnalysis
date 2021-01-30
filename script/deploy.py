#!/usr/bin/env python
# -*- coding:utf-8 -*-

import datetime
import os
import sys
from pyhocon import ConfigFactory

data_file = sys.argv[1] if len(sys.argv) > 1 else None
conf = ConfigFactory.parse_file('src/main/resources/application.conf')
hosts = conf['wzx.deploy.cluster']
master = conf['wzx.deploy.master']
remote_data_path = conf['wzx.deploy.data_path']


def exec_ssh_cmd(host_name: str, cmd: str):
    os.system(f"ssh {host_name} 'cd {remote_data_path};{cmd}'")


def exe_master_cmd(cmd: str):
    exec_ssh_cmd(master, cmd)


def upload_file_remote_data_path(host_name: str, local_file: str):
    os.system(f'scp {local_file} {host_name}:{remote_data_path}')


def upload_file_master_data_path(local_file: str):
    upload_file_remote_data_path(master, local_file)


# 创建data path
for host in hosts:
    exec_ssh_cmd(host, f"mkdir -p {remote_data_path}")
    exec_ssh_cmd(host, f"mkdir -p {remote_data_path}/log")
    print(f"make directory on {host}")

# 数据分发
os.system(f'python3 script/create_dataset.py {data_file} {len(hosts) + 1}')
weblogs_path = os.path.join(remote_data_path, "weblogs")
for index, host in enumerate(hosts + ['localhost']):
    upload_file_remote_data_path(host, f'tmp_{index}')
    print(f'tmp_{index} send to {host}:{weblogs_path}')
    os.remove(f'tmp_{index}')

# 编译上传jar包
print('build and upload package')
ret = os.system('mvn package')
if ret != 0:
    raise RuntimeError("maven build error")
for host in hosts:
    upload_file_remote_data_path(host, "target/LabelLogAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar")

# 上传ip解析包
print('upload ip2region')
for host in hosts:
    upload_file_remote_data_path(host, "src/main/resources/ip2region.db")

# 上传sql, script, 配置文件到master服务器上
print('upload script and sql file')
upload_file_master_data_path(
    'sql/* script/topic.py script/submit.py script/crontab.py src/main/resources/application.conf')

# 创建topic
print('create kafka topic')
exe_master_cmd('python3 topic.py delete')
exe_master_cmd('python3 topic.py create')

# 创建数据表
print('create database table')
cur_month = datetime.date.today().replace(day=1).strftime("%Y-%m-%d %H:%M:%S")
exe_master_cmd(
    f'impala-shell -f impala.sql --var=cur_month="{cur_month}"')

# 离线sql作业和sql作业, crontab
print('create crontab')
exe_master_cmd('python3 crontab.py')

# 提交实时Flink作业
print('submit flink job')
exe_master_cmd('python3 submit.py flink')
