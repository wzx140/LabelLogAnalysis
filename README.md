## 环境搭建
1. CDH Manager，参考[官方教程](https://docs.cloudera.com/documentation/enterprise/6/6.0/topics/installation.html)
2. 在CDH中添加Hive, Zookeeper, HDFS, Spark, Flink, Kafka, Yarn, Kudu
3. 编译zeppelin与livy的parcels和csd, 参考[livy_zeppelin_cdh_csd_parcels](https://github.com/alexjbush/livy_zeppelin_cdh_csd_parcels)
4. zeppelin与livy与CDH集成，参考[博客](https://www.itocm.com/a/3C84D18AE81B46BC80CF4AB64C8159F6)

## 功能
- 实时用户标签
- 离线用户标签
- olap分析
- bitmap优化标签join

## 架构
- etl: mock -> kafka -> flink -> kudu
- 实时标签: kafka -> flink -> kudu
- 离线标签: crontab -> spark -> hive
- kudu sink: crontab -> kudu -> spark -> hive
- spark udf: bitmap_filter(col, label...)
