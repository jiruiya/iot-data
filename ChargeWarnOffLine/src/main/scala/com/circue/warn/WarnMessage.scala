package com.circue.warn

import com.circue.utils.DateUtil
import org.apache.spark.sql._

import scala.collection.mutable.ListBuffer

object WarnMessage {
  private val warnType1: List[String] = List("bsm_single_volt_status", "bsm_soc_status", "bsm_current_status")

  private val warnType2: List[String] = List("bsm_charge_status")

  private val warnType3: List[String] = List("bsm_temp_status", "bsm_insulate_status", "bsm_connect_status",
    "bst_insulation_status", "bst_conn_over_temp_status", "bst_bms_over_temp_status", "bst_charge_conn_status",
    "bst_battery_pack_temp_status", "bst_high_volt_relay_status", "bst_charging_port_volt_status", "bst_current_status",
    "bst_volt_status", "bst_other_status", "cst_temp_status", "cst_connector_status", "cst_internal_temp_status",
    "cst_power_status", "cst_emergency_stop_status", "cst_current_status", "cst_volt_status", "cst_other_status")
  private val warnType4: List[String] = List("bem_error", "cem_error")


  /**
   * 标记报警
   *
   * @param value 解析的报文数据
   */
  def catchWarnMessage(value: Row): List[Map[String, String]] = {

    val order_id: String = value.getAs[String]("order_id")
    val device_id: String = value.getAs[String]("device_id")
    val maybeLong: Option[Long] = DateUtil.time2Timestamp(value.getAs[String]("abs_time"))
    val absTime: Long = if (maybeLong.isEmpty) 0 else maybeLong.get
    val list: ListBuffer[Map[String, String]] = ListBuffer()
    for (warnItem: String <- warnType1) {
      if (List("1", "2").contains(value.getAs[String](warnItem))) {
        list += Map(
          "orderId" -> order_id,
          "warnTime" -> absTime.toString,
          "warnName" -> warnItem,
          "warnLevel" -> "1",
          "device_id" -> device_id
        )
      }
    }

    for (warnItem: String <- warnType2) {
      if (value.getAs[String](warnItem) == "0") {
        list += Map(
          "orderId" -> order_id,
          "warnTime" -> absTime.toString,
          "warnName" -> warnItem,
          "warnLevel" -> "1",
          "device_id" -> device_id
        )
      }
    }

    for (warnItem: String <- warnType3) {
      if (value.getAs[String](warnItem) == "1") {
        list += Map(
          "orderId" -> order_id,
          "warnTime" -> absTime.toString,
          "warnName" -> warnItem,
          "warnLevel" -> "1",
          "device_id" -> device_id
        )
      }
    }

    for (warnItem: String <- warnType4) {
      if (value.getAs[String](warnItem) != null) {
        list += Map(
          "orderId" -> order_id,
          "warnTime" -> absTime.toString,
          "warnName" -> warnItem,
          "warnLevel" -> "1",
          "device_id" -> device_id
        )
      }
    }

    list.toList
  }
}
