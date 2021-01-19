## 环境搭建
1. 安装CDH Manager，参考[官方教程](https://docs.cloudera.com/documentation/enterprise/6/6.0/topics/installation.html)
2. 在CDH中添加Flink, HDFS, Hive, Kafka, Kudu, Spark, YARN, ZooKeeper
3. 编译zeppelin与livy的parcels和csd, 参考[livy_zeppelin_cdh_csd_parcels](https://github.com/alexjbush/livy_zeppelin_cdh_csd_parcels)
4. zeppelin与livy与CDH集成，参考[博客](https://www.itocm.com/a/3C84D18AE81B46BC80CF4AB64C8159F6)
5. [编译Flink Kudu Connector](https://github.com/apache/bahir-flink)

## 功能
- 实时etl
- 离线用户标签
- olap分析

## 架构
- 实时etl: mock -> kafka -> flink -> kudu
- 离线标签: crontab -> spark -> hive
- kudu sink: crontab -> impala sql -> hive

## 集群配置
- master
    - Mysql
    - HDFS NameNode
    - HDFS SecondaryNameNode
    - ZooKeeper Server
    - Kudu Master
    - YARN Resource Manager
    - Hive Metastore Server
    - HiveServer2
    - Livy REST Server
    - Zeppelin Server
    - Impala Catalog Server
    - Impala StateStore
- slave1:
    - HDFS DataNode
    - Kafka Broker
    - YARN Node Manager
    - Kudu Tablet Server
    - ZooKeeper Server
    - Impala Daemon
- slave2:
    - HDFS DataNode
    - Kafka Broker
    - YARN Node Manager
    - Kudu Tablet Server
    - ZooKeeper Server
    - Impala Daemon
  
## 日志格式
```
60.165.39.1 - - [10/Nov/2016:00:01:53 +0800] "POST /course/ajaxmediauser HTTP/1.1" 200 54 "www.imooc.com" "http://www.imooc.com/code/1431" mid=1431&time=60 "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 Safari/537.36 SE 2.X MetaSr 1.0" "-" 10.100.136.64:80 200 0.014 0.014
14.145.74.175 - - [10/Nov/2016:00:01:53 +0800] "POST /course/ajaxmediauser/ HTTP/1.1" 200 54 "www.imooc.com" "http://www.imooc.com/video/678" mid=678&time=60&learn_time=551.5 "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36" "-" 10.100.136.64:80 200 0.014 0.014
```

百度云盘下载地址：链接：https://pan.baidu.com/s/1VfOG14mGW4P4kj20nzKx8g 提取码：uwjg

## 表
- WOS: Write Optimized Store(kudu)
- ROS: Read Optimized Store(parquet)