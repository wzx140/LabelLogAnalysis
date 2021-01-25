#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys

command = sys.argv[1] if len(sys.argv) > 1 else None

# todo 资源参数
# todo 队列
spark_submit_cmd = """spark-submit \
--conf spark.yarn.maxAppAttempts=1 \
--queue root.production.miot_group.youpin.data \
--master yarn \
--deploy-mode cluster \
--num-executors 20 \
--driver-memory 4g \
--executor-memory 4g \
--conf spark.default.parallelism=60 \
--class {{class_name}} \
target/LabelLogAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar"""

# todo 资源参数
# todo 队列
flink_submit_cmd = """spark-submit \
--conf spark.yarn.maxAppAttempts=1 \
--queue root.production.miot_group.youpin.data \
--master yarn \
--deploy-mode cluster \
--num-executors 20 \
--driver-memory 4g \
--executor-memory 4g \
--conf spark.default.parallelism=60 \
--class {{class_name}} \
target/LabelLogAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar"""

if command == 'flink':
    os.system(flink_submit_cmd.format(class_name='com.wzx.streaming.DataClean'))
elif command == 'spark_video_visit_over_100':
    os.system(spark_submit_cmd.format(class_name='com.wzx.extracting.VideoVisitOver100'))
elif command == 'spark_new_register':
    os.system(spark_submit_cmd.format(class_name='com.wzx.extracting.NewRegisterExtract'))
