package com.wzx.entity

case class Event(
    // url
    url: String,
    // 访问类型
    cms_type: String,
    // 访问类型对应的编号
    cms_id: Int,
    // 流量
    traffic: Int,
    // 用户ip
    ip: String,
    // yyyy-MM-dd HH:mm:ss
    time: String
)
