package com.wzx.util

import java.time.LocalDateTime

object OptionUtil {

  def getTime(args: Array[String]): (LocalDateTime, LocalDateTime) = {
    // 解析日期
    val date = DateUtil.parseDayFormat(
      args.headOption
        .getOrElse(DateUtil.formatDay(LocalDateTime.now()))
    )
    val beforeDate = date.minusDays(1)

    (date, beforeDate)
  }
}
