package com.circue.utils

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}

/**
 * 日期转换工具函数
 */
object DateUtil {
  val originFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")      // 源日期时间格式
  val targetFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss") // 目的日期时间格式
  val dayFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd") // 目的日期时间格式



  /**
   * 时间转时间戳
   * @param timeString 字符串时间

   * @return
   */
  def time2Timestamp(timeString: String): Option[Long] = {
    try {
      val dateTime: LocalDateTime = LocalDateTime.parse(timeString, originFormat)
      Some(dateTime.toEpochSecond(ZoneOffset.ofHours(8)))
    } catch {
      case _: Exception =>
        None

    }
  }

    /**
     * 时间戳转字符串时间
     * @param timestamp 时间戳
     * @param zoneId 当地时间
     * @return
     */
    def timestamp2Time(timestamp: Long, zoneId: ZoneId = ZoneId.systemDefault()): String = {
      var time:Long = 0
      try {
        if(timestamp.toString.length==10){
          time = timestamp*1000
        }else{
          time = timestamp
        }
        val instant: Instant = Instant.ofEpochMilli(time)
        val formatter: DateTimeFormatter = targetFormat.withZone(zoneId)
        formatter.format(instant)
      } catch {
        case _: Exception =>
          ""
      }
    }


  /**
   * 把日期字符串转换为 10 位的 unix 时间戳
   *
   * @param timeStr 日期字符串
   */
  def unixTimestamp(timeStr: String, formatter: DateTimeFormatter): String = {
    try {
      if (timeStr.length == 14) {
        val ts = LocalDateTime.parse(timeStr, formatter) // 起始日期时间
        ts.toEpochSecond(ZoneOffset.ofHours(8)).toString
      } else {
        ""
      }
    } catch {
      case ex: Exception => println(ex)
        ""
    }
  }

  /**
   * 从订单编号中截取日期字符串并转换为 yyyy-MM-dd HH:mm:ss 格式
   *
   * @param orderId  订单编号
   * @param deviceId 设备编号, 用于计算截取日期字符串的起始位置
   * @param relTime  相对时间, 最多 6 位数
   * @return 返回 yyyy-MM-dd HH:mm:ss 格式的字符串
   */
  def chargeStartEndTime(orderId: String, deviceId: String, relTime: String): String = {
    // 使用 orderId 和 relTime 拼接每条数据的时间戳
    // 样例数据: "orderId": "120201006223007712", "relTime": "000036"
    // 样例数据: "orderId": "1020201006223007712", "relTime": "000036"
    // start: length(deviceId), end: 14 + length(deviceId), 因为 20201026221303 的长度为 14
    val startingOffset: Int = deviceId.length // 设备编号的长度
    val endingOffset: Int = 14 + startingOffset // 截取的结束位置

    try {
      val offsetSeconds: Int = relTime.toInt // 偏移的秒数
      val dateTimeInOrderId: String = orderId.substring(startingOffset, endingOffset) // 订单编号中的日期时间
      val startDateTime: LocalDateTime = LocalDateTime.parse(dateTimeInOrderId, DateUtil.originFormat) // 起始日期时间
      val startDateTimePlusOffset: LocalDateTime = startDateTime.plusSeconds(offsetSeconds) // 起始日期时间加上偏移的秒数
      startDateTimePlusOffset.format(DateUtil.targetFormat) // 当前 relTime 的时间
    } catch {
      case ex: Exception =>
        println("orderId: ", orderId, deviceId, ex)
        ""
    }
  }

  /**
   * 从订单编号中截取日期字符串并转换为 Unix 时间戳
   *
   * @param orderId  订单编号
   * @param deviceId 设备编号, 用于计算截取日期字符串的起始位置
   * @return 13 位的 Unix 时间戳字符串
   */
  def stringToUnixTime(orderId: String, deviceId: String): String = {
    // 取 orderId 中的日期字符串并转换为 UnixTime
    // 样例数据: "orderId": "120201006223007712"
    // 样例数据: "orderId": "1020201006223007712"
    // start: length(deviceId), end: 14 + length(deviceId), 因为 20201026221303 的长度为 14
    val startingOffset: Int = deviceId.length // 设备编号的长度
    val endingOffset: Int = 14 + startingOffset // 截取的结束位置

    try {
      val dateTimeInOrderId: String = orderId.substring(startingOffset, endingOffset) // 订单编号中的日期时间
      val startDateTime: LocalDateTime = LocalDateTime.parse(dateTimeInOrderId, DateUtil.originFormat) // 起始日期时间
      startDateTime.toEpochSecond(ZoneOffset.ofHours(8)).toString + "000" // 补成 13 位的 unix 时间戳
    } catch {
      case ex: Exception =>
        println("wrong orderId and deviceId: ", orderId, deviceId, ex)
        ""
    }
  }

  def unixTimeToDate(ts: String): String = {
    if (ts != "") {
      val unixTimestamp: LocalDateTime = LocalDateTime.ofEpochSecond(ts.toLong, 0, ZoneOffset.ofHours(8))
      unixTimestamp.format(targetFormat)
    } else {
      "1970-01-01 00:00:00"
    }
  }

  /**
   * 获取当天日期
   *
   * @param strTime 输入时间
   * @return 日期
   */
  def getDay(strTime: String): String = {
    getDay(strTime, targetFormat)
  }

  /**
   * 获取当天日期
   *
   * @param strTime 输入时间
   * @return 日期
   */
  def getDay(strTime: String,format:DateTimeFormatter): String = {
    try {
      val originDate: LocalDateTime = LocalDateTime.parse(strTime, format)
      originDate.format(DateUtil.dayFormat)
    } catch {
      case ex: Exception =>
        ""
    }
  }


}
