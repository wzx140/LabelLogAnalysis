#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys

command = sys.argv[1] if len(sys.argv) > 1 else None
param = sys.argv[2:] if len(sys.argv) > 2 else ""

spark_submit_cmd = """spark-submit \
--conf spark.yarn.maxAppAttempts=1 \
--master yarn \
--deploy-mode cluster \
--num-executors 2 \
--driver-memory 1g \
--executor-memory 1g \
--conf spark.default.parallelism=2 \
--class {class_name} \
LabelLogAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar \
{param}
"""

flink_submit_cmd = """flink run \
--jobmanager yarn-cluster \
--yarncontainer 2 \
--yarnjobManagerMemory 1024 \
--yarntaskManagerMemory 1024 \
--yarnname {class_name} \
--class {class_name} \
--parallelism 2 \
--detached \
LabelLogAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar \
{param}
"""

if command == 'flink':
    os.system(flink_submit_cmd.format(class_name='com.wzx.streaming.DataExtract', param=" ".join(param)))
elif command == 'spark_video_visit_over_100':
    os.system(spark_submit_cmd.format(class_name='com.wzx.extracting.VideoVisitOver100', param=" ".join(param)))
elif command == 'spark_new_register':
    os.system(spark_submit_cmd.format(class_name='com.wzx.extracting.NewRegisterExtract', param=" ".join(param)))
