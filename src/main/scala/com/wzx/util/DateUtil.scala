package com.wzx.util

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

object DateUtil {

  private val SLASH_TIME_FORMAT =
    DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  private val LINE_TIME_FORMAT =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
  private val DAY_FORMAT =
    DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH)

  def parseSlashFormat(time: String): LocalDateTime = {
    LocalDateTime.parse(time, SLASH_TIME_FORMAT)
  }

  def parseLineFormat(time: String): LocalDateTime = {
    LocalDateTime.parse(time, LINE_TIME_FORMAT)
  }

  def formatSlash(dateTime: LocalDateTime): String = {
    SLASH_TIME_FORMAT.format(dateTime)
  }

  def formatLine(dateTime: LocalDateTime): String = {
    LINE_TIME_FORMAT.format(dateTime)
  }

  def getDay(dateTime: LocalDateTime): String = {
    DAY_FORMAT.format(dateTime)
  }
}
