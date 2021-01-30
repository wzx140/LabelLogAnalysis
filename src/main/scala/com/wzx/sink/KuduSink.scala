package com.wzx.sink

import com.wzx.sink.KuduSink.log
import com.wzx.util.TransformUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{
  RichSinkFunction,
  SinkFunction
}
import org.apache.kudu.client.{
  KuduClient,
  KuduSession,
  KuduTable,
  RowError,
  SessionConfiguration
}
import org.slf4j.LoggerFactory
import collection.JavaConverters._

object KuduSink {
  private val name = this.getClass.getName.stripSuffix("$")
  private val log = LoggerFactory.getLogger(name)
}

class KuduSink[T](
    private val kuduMaster: String,
    private val tableName: String,
    private val timeout: Int
) extends RichSinkFunction[T] {

  private var client: KuduClient = _
  private var table: KuduTable = _
  private var session: KuduSession = _

  private def checkAsyncErrors(): Unit = {
    if (session.countPendingErrors != 0) {
      val failures = session.getPendingErrors.getRowErrors.toList
      handleError(failures)
    }
  }

  private def handleError(failures: List[RowError]): Unit = {
    val (_, otherFailures) =
      failures.partition(_.getErrorStatus.isAlreadyPresent)

    if (otherFailures.nonEmpty) {
      log.error(s"Error while sending value. \n ${failures.mkString("\n")}")
    }
  }

  override def open(parameters: Configuration): Unit = {
    client = new KuduClient.KuduClientBuilder(kuduMaster)
      .defaultAdminOperationTimeoutMs(timeout * 1000)
      .defaultOperationTimeoutMs(timeout * 1000)
      .build()
    table = client.openTable(tableName)
    session = client.newSession()
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND)
    log.info(
      s"open kudu table $tableName: \n ${table.getSchema.getColumns.asScala.mkString("\n")}"
    )
  }

  override def invoke(value: T, context: SinkFunction.Context[_]): Unit = {
    val insert = table.newInsert()
    val row = insert.getRow
    TransformUtil.addRow(value, row)

    val res = session.apply(insert)
    if (res != null && res.hasRowError) {
      handleError(List(res.getRowError))
    } else {
      checkAsyncErrors()
      log.debug(s"insert into table $tableName: $value")
    }
  }

  override def close(): Unit = {
    checkAsyncErrors()
    session.flush()
    checkAsyncErrors()
    session.close()
    client.close()
    log.info(s"close kudu table $tableName")
  }
}
