package com.wzx.util

import com.wzx.common.FilePath
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.Locale

object DateUtil {
  private val LINE_TIME_FORMAT =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
  private val DAY_FORMAT =
    DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH)

  def parseLineFormat(time: String): LocalDateTime = {
    LocalDateTime.parse(time, LINE_TIME_FORMAT)
  }

  def parseDayFormat(time: String): LocalDateTime = {
    LocalDate.parse(time, DAY_FORMAT).atStartOfDay()
  }

  def formatLine(dateTime: LocalDateTime): String = {
    LINE_TIME_FORMAT.format(dateTime)
  }

  def formatDay(dateTime: LocalDateTime): String = {
    DAY_FORMAT.format(dateTime)
  }

  /**
    * 获得上个月第一天
    */
  def getPrevMonth(dateTime: LocalDateTime): LocalDateTime = {
    val prevMonth = dateTime.minusMonths(1)

    LocalDateTime.of(prevMonth.getYear, prevMonth.getMonth, 1, 0, 0, 0)
  }

  def formatDateString(
      src: String,
      dateTime: LocalDateTime
  ): String = {
    src
      .replaceAll(FilePath.YEAR_PATTERN, dateTime.getYear.toString)
      .replaceAll(FilePath.MONTH_PATTERN, dateTime.getMonth.getValue.toString)
      .replaceAll(FilePath.DAY_PATTERN, dateTime.getDayOfMonth.toString)
  }
}
