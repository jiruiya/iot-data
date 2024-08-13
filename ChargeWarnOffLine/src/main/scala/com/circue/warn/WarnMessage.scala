package com.circue.alarm.warn.pile

import com.circue.alarm.common.utils.DateUtil
import com.circue.alarm.warn.beans.WarnMarker

object WarnMessage {
  private val warnType1: List[String] = List("bsm_singleVoltStatus", "bsm_socStatus", "bsm_currentStatus")

  private val warnType2: List[String] = List("bsm_chargeStatus")

  private val warnType3: List[String] = List("bsm_tempStatus", "bsm_insulateStatus", "bsm_connectStatus", "bst_insulationStatus",
    "bst_connOverTempStatus", "bst_bmsOverTempStatus", "bst_chargeConnStatus",
    "bst_batteryPackTempStatus", "bst_highVoltRelayStatus", "bst_chargingPortVoltStatus",
    "bst_currentStatus", "bst_voltStatus", "bst_otherStatus", "cst_tempStatus", "cst_connectorStatus",
    "cst_internalTempStatus", "cst_powerStatus", "cst_emergencyStopStatus", "cst_currentStatus",
    "cst_voltStatus", "cst_otherStatus")
  private val warnType4: List[String] = List("bem_error", "cem_error")


  /**
   * 标记报警
   *
   * @param value 解析的报文数据
   */
  def catchWarnMessage(value: Map[String, String]): List[WarnMarker] = {
    val deviceId: String = value.getOrElse("deviceId", "")
    val orderId: String = value.getOrElse("orderId", "")

    val maybeLong: Option[Long] = DateUtil.time2Timestamp(value.getOrElse("absTime", "0"))
    val absTime: Long = if (maybeLong.isEmpty) 0 else maybeLong.get
    value.collect {
      // 0 = 正常, 1 = 过高, 2 = 过低
      case value if warnType1.contains(value._1) &&
        List("1", "2").contains(value._2) =>
        WarnMarker(
        deviceId = deviceId,
        markerTime = absTime,
        markerItem = value._1,
        orderId = orderId
      )

      // 0 = 禁止, 1 = 允许
      case value if warnType2.contains(value._1) && value._2 == "0" =>
        WarnMarker(
        deviceId = deviceId,
        markerTime = absTime,
        markerItem = value._1,
        orderId = orderId
      )

      // 0 = 正常, 1 = 故障, 2 = 不可信状态
      case value if warnType3.contains(value._1) && value._2 == "1" =>
        WarnMarker(
          deviceId = deviceId,
          markerTime = absTime,
          markerItem = value._1,
          orderId = orderId
        )

      case value if warnType4.contains(value._1) =>
        WarnMarker(
        deviceId = deviceId,
        markerTime = absTime,
        markerItem = value._1,
        orderId = orderId
      )
    }.toList
  }
}
