package com.wzx.util

import java.time.LocalDateTime

object OptionUtil {

  def getTime(args: Array[String]): (LocalDateTime, LocalDateTime) = {
    if (args.length < 1)
      throw new IllegalArgumentException(
        s"${args.mkString(",")} missing date"
      )
    // 解析日期
    val date = DateUtil.parseDayFormat(args(0))
    val beforeDate = date.minusDays(1)

    (date, beforeDate)
  }
}
