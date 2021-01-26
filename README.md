# 基于标签的用户日志分析系统

## 架构
- 实时etl: mock Kafka uploader -> Kafka -> Flink -> Kudu。实时处理日志数据流，生成事件和用户属性
  - event: 用户事件
  - profile: 用户属性
- 离线标签: Crontab -> Spark -> Parquet。每天凌晨自动跑的两个离线任务，生成对应用户标签
  - 近一周新注册的用户
  - 当天video访问量超过100的用户
- 滑动窗口: Crontab -> Impala SQL。存储分层，三个任务每个月执行一次
  - 数据移动: [kudu -> parquet](sql/window_data_move.sql)
  - 分区移动: [drop and add kudu range partition](sql/window_partition_shift.sql)
  - 视图移动: [alter kudu view](sql/window_view_alter.sql)
- [数据表结构](sql/impala.sql)

## 滑动窗口模式
### 技术选型
- HDFS Parquet: 列式存储结构
  - 适合OLAP场景，只读取需要的列，**更小的IO操作**
  - 适合存储历史大容量的，列式存储使得每个列高效的压缩和编码，**更高地压缩比**
- Kudu: HBase低延迟的记录级别随机读写与HDFS高吞吐量连续读取数据的能力的**平衡点**
  - 低延迟的更新，适用于**实时数据的快速入库**
  - 接近于Parquet的批量扫描性能，适用于**OLAP分析**
  - 快速插入更新，适用于**维度表**

### 存储分层
![](img/pic1.jpg)
- Kudu: 保存一个月的Event数据
- HDFS: 保存剩下的历史数据
- Boundary: 滑动窗口的边界
- VIEW: Kudu和HDFS的统一视图

## 日志
原始日志只有一天的数据量，格式如下
```
60.165.39.1 - - [10/Nov/2016:00:01:53 +0800] "POST /course/ajaxmediauser HTTP/1.1" 200 54 "www.imooc.com" "http://www.imooc.com/code/1431" mid=1431&time=60 "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 Safari/537.36 SE 2.X MetaSr 1.0" "-" 10.100.136.64:80 200 0.014 0.014
14.145.74.175 - - [10/Nov/2016:00:01:53 +0800] "POST /course/ajaxmediauser/ HTTP/1.1" 200 54 "www.imooc.com" "http://www.imooc.com/video/678" mid=678&time=60&learn_time=551.5 "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36" "-" 10.100.136.64:80 200 0.014 0.014
```
链接: https://pan.baidu.com/s/169yznx9QOyMQcOoEL55f2Q  密码: 47vc

`create_dataset.py`会根据集群数量自动切分原始日志，并将时间修改为近两个月的均匀分布

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
    - Hue Server
    - Impala Catalog Server
    - Impala StateStore
    - Impala Daemon
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

## 部署
1. 安装CDH Manager，参考[官方教程](https://docs.cloudera.com/documentation/enterprise/6/6.0/topics/installation.html)
2. 在CDH中添加Flink, HDFS, Hive, Kafka, Kudu, Spark, YARN, ZooKeeper, Hue; HDFS注意关闭权限检查
3. [编译Flink Kudu Connector](https://github.com/apache/bahir-flink)
4. `pip3 install python-crontab`
5. 下载[日志](#日志)
6. `python3 script/deploy.py path_of_log slave1 slave2`
    - 根据原始日志进行时间更改和切分，并上传到本机和指定的host
    - 编译jar包并上传HDFS
    - 创建Kafka topic
    - 创建Impala数据表
    - 创建crontab离线sql作业
    - 提交实时Flink作业
    - 创建crontab离线Spark作业
    - 本机和指定的host，开启Mock Kafka Uploader
 
## 参考
1. [mooc日志分析系统](https://github.com/whirlys/BigData-In-Practice/tree/master/ImoocLogAnalysis)
2. [使用Apache Kudu和Impala实现存储分层](https://my.oschina.net/dabird/blog/3051625)