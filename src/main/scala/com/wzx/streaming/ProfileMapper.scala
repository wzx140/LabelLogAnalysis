package com.wzx.streaming

import com.wzx.entity.{Event, Profile}
import com.wzx.util.{DateUtil, IpUtil}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class ProfileMapper extends RichFlatMapFunction[Event, Profile] {

  private var state: MapState[String, Boolean] = _

  override def open(parameters: Configuration): Unit = {
    val descriptor = new MapStateDescriptor[String, Boolean](
      "profile",
      classOf[String],
      classOf[Boolean]
    )
    // 设置ttl
    val ttlConfig =
      StateTtlConfig
        .newBuilder(Time.hours(1))
        // 何时更新过期状态
        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
        // 是否可以访问过期数据
        .setStateVisibility(
          StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp
        )
        .build()
    descriptor.enableTimeToLive(ttlConfig)
    // 获取map state
    state = getRuntimeContext.getMapState(descriptor)
  }

  override def flatMap(value: Event, out: Collector[Profile]): Unit = {
    if (!state.contains(value.ip)) {
      state.put(value.ip, false)
      // parse ip
      val city = IpUtil.getCity(value.ip)
      val day = DateUtil.formatDay(DateUtil.parseLineFormat(value.time))
      val profile = Profile(value.ip, city, day)
      out.collect(profile)
    }
  }
}
