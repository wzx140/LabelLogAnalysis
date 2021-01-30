#!/usr/bin/env python
# -*- coding:utf-8 -*-

from pyhocon import ConfigFactory
import os

conf = ConfigFactory.parse_file('application.conf')
remote_data_path = conf['wzx.deploy.data_path']

crontab = f"""0 1 1 * * cd {remote_data_path};impala-shell -f window_data_move.sql --var=cur_month="$(date +"\%Y-\%m-01 00:00:00")" &>> {remote_data_path}/log/data-move.log
30 1 1 * * cd {remote_data_path};impala-shell -f window_partition_shift.sql --var=cur_month="$(date +"\%Y-\%m-01 00:00:00")" &>> {remote_data_path}/log/data-shift.log
0 2 1 * * cd {remote_data_path};impala-shell -f window_view_alter.sql --var=cur_month="$(date +"\%Y-\%m-01 00:00:00")" &>> {remote_data_path}/log/data-alter.log
0 0 * * * cd {remote_data_path};python3 submit.py spark_video_visit_over_100 &>> {remote_data_path}/log/spark-submit.log
30 0 * * * cd {remote_data_path};python3 submit.py spark_new_register &>> {remote_data_path}/log/spark-submit.log
0 1 * * * hive -e "msck repair table user_tag_1_ros";impala-shell -q "INVALIDATE METADATA user_tag_1_ros;describe user_tag_1_ros;"
10 1 * * * hive -e "msck repair table user_tag_2_ros";impala-shell -q "INVALIDATE METADATA user_tag_2_ros;describe user_tag_2_ros;"
"""

if not os.path.exists(f'crontab.bak'):
    with open("crontab.bak", "w") as text_file:
        text_file.write(crontab)

os.system('crontab crontab.bak')
