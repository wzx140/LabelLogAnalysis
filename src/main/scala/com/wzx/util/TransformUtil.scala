package com.wzx.util

import com.wzx.entity.{Event, Profile}
import org.apache.kudu.client.PartialRow

object TransformUtil {

  def addRow[T](obj: T, row: PartialRow): Unit = {
    obj match {
      case Event(url, cms_type, cms_id, traffic, ip, time) =>
        row.addString("url", url)
        row.addString("time", time)
        row.addString("cms_type", cms_type)
        row.addInt("cms_id", cms_id)
        row.addInt("traffic", traffic)
        row.addString("ip", ip)

      case Profile(ip, city, register_day)=>
        row.addString("ip", ip)
        row.addString("city", city)
        row.addString("register_day", register_day)
    }
  }

}
