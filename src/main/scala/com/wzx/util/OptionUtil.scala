package com.wzx.util

import java.time.LocalDateTime

object OptionUtil {

  /**
   * 解析命令行参数时间及其前一天, 默认为当前时间
   */
  def getTime(args: Array[String]): (LocalDateTime, LocalDateTime) = {
    val date = DateUtil.parseDayFormat(
      args.headOption
        .getOrElse(DateUtil.formatDay(LocalDateTime.now()))
    )
    val beforeDate = date.minusDays(1)

    (date, beforeDate)
  }
}
