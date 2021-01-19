package com.wzx.streaming

import org.apache.flink.connectors.kudu.connector.failure.KuduFailureHandler
import org.apache.kudu.client.RowError
import org.slf4j.Logger
import java.io.IOException
import java.util
import collection.JavaConverters._

class LogFailureHandler(logger: Logger) extends KuduFailureHandler {

  private val log: Logger = logger

  override def onFailure(failure: util.List[RowError]): Unit = {
    val errors = failure.asScala
    val errorsString = errors
      .map(error => error.toString)
      .mkString(System.lineSeparator)

    if (errors.exists(error => error.getErrorStatus.isAlreadyPresent)) {
      log.info(s"profile already exist. \n $errorsString")
    } else {
      throw new IOException(s"Error while sending value. \n $errorsString")
    }
  }
}
