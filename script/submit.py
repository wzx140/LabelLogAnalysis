#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys

command = sys.argv[1] if len(sys.argv) > 1 else None

spark_submit_cmd = """spark-submit \
--conf spark.yarn.maxAppAttempts=1 \
--master yarn \
--deploy-mode cluster \
--num-executors 2 \
--driver-memory 1g \
--executor-memory 1g \
--conf spark.default.parallelism=2 \
--class {class_name} \
target/LabelLogAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar"""

flink_submit_cmd = """flink run \
--jobmanager yarn-cluster \
--yarncontainer 3 \
--yarnjobManagerMemory 1024 \
--yarntaskManagerMemory 1024 \
--class {class_name} \
--parallelism 3 \
--detached \
target/LabelLogAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar
"""

if command == 'flink':
    os.system(flink_submit_cmd.format(class_name='com.wzx.streaming.DataClean'))
elif command == 'spark_video_visit_over_100':
    os.system(spark_submit_cmd.format(class_name='com.wzx.extracting.VideoVisitOver100'))
elif command == 'spark_new_register':
    os.system(spark_submit_cmd.format(class_name='com.wzx.extracting.NewRegisterExtract'))
