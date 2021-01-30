package com.wzx.util

import com.typesafe.config.{Config, ConfigFactory}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}
import java.nio.file.Paths

object IpUtil {

  val config: Config = ConfigFactory.load("application.conf")
  var dbPath: String =
    Paths.get(config.getString("wzx.deploy.data_path"), "ip2region.db").toString

  @transient
  private val searcher = new DbSearcher(new DbConfig(), dbPath)

  def getCity(ip: String): String = {
    val city = searcher.btreeSearch(ip).getRegion.split("\\|")(2)

    if (city == "0") "" else city
  }
}
