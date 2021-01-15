package com.wzx.entity

case class Event(
    // url
    url: String,
    // 访问类型
    cmsType: String,
    // 访问类型对应的编号
    cmsId: Int,
    // 流量
    traffic: Int,
    // 用户ip
    ip: String,
    // 用户城市
    city: String,
    // yyyy-MM-dd HH:mm:ss
    time: String,
    // yyyy-MM-dd
    day: String
)
