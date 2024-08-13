package com.circue

import com.circue.utils.DateUtil.{targetFormat, timestamp2Time}
import com.circue.warn.WarnMessage
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf, when}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object WarnApp {

  private val levelMap: Map[String, String] = Map(
    "bsm_singleVoltStatus" -> "2",
    "bsm_socStatus" -> "2",
    "bsm_currentStatus" -> "2",
    "bsm_tempStatus" -> "2",
    "bsm_insulateStatus" -> "2",
    "bsm_connectStatus" -> "2",
    "bsm_chargeStatus" -> "1",
    "bst_insulationStatus" -> "2",
    "bst_connOverTempStatus" -> "2",
    "bst_bmsOverTempStatus" -> "2",
    "bst_chargeConnStatus" -> "2",
    "bst_batteryPackTempStatus" -> "2",
    "bst_highVoltRelayStatus" -> "2",
    "bst_chargingPortVoltStatus" -> "1",
    "bst_currentStatus" -> "2",
    "bst_voltStatus" -> "2",
    "bem_error" -> "1",
    "bst_otherStatus" -> "1",
    "cst_tempStatus" -> "2",
    "cst_connectorStatus" -> "2",
    "cst_internalTempStatus" -> "2",
    "cst_powerStatus" -> "2",
    "cst_emergencyStopStatus" -> "2",
    "cst_currentStatus" -> "1",
    "cst_voltStatus" -> "2",
    "cem_error" -> "1",
    "cst_otherStatus" -> "1")

  def main(args: Array[String]): Unit = {


    val startDay: String = args(1)
    val endDay: String = args(2)
    val timeDiff: Int = args(3).toInt
    val dataSize: Int = args(4).toInt
    val path: String = s"${args(0)}_${dataSize}_$timeDiff"

    println(path)
    println(startDay)
    println(endDay)


    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val spark: SparkSession = SparkSession
      .builder
      .appName("dwd_mcd_power_station_electricity_price_detail_di_%s")
      //      .master("local[*]")
      .config("spark.local.dir", "tmp")
      .config("hive.metastore.uris", "thrift://10.0.0.161:9083")
      .config("spark.sql.warehouse.dir", "hdfs://ns1/user/hive/warehouse/")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.max.dynamic.partitions.pernode", "10000")
      .config("hive.exec.max.dynamic.partitions", "100000")
      .config("hive.exec.max.created.files", "150000")
      .config("hive.security.authorization.enabled", "false")
      .config("hive.security.authorization.manager", "")
      .enableHiveSupport
      .getOrCreate
    spark.sparkContext.setLogLevel("ERROR")
    val warn_sql: String =
      """ (bsm_single_volt_status NOT IN ("1","2")
        |       OR bsm_soc_status NOT IN ("1","2")
        |       OR bsm_current_status NOT IN ("1","2")
        |       OR bsm_charge_status != "0"
        |       OR bsm_temp_status = "1"
        |       OR bsm_insulate_status = "1"
        |       OR bsm_connect_status = "1"
        |       OR bst_insulation_status = "1"
        |       OR bst_conn_over_temp_status = "1"
        |       OR bst_bms_over_temp_status = "1"
        |       OR bst_charge_conn_status = "1"
        |       OR bst_battery_pack_temp_status = "1"
        |       OR bst_high_volt_relay_status = "1"
        |       OR bst_charging_port_volt_status = "1"
        |       OR bst_current_status = "1"
        |       OR bst_volt_status = "1"
        |       OR bst_other_status = "1"
        |       OR cst_temp_status = "1"
        |       OR cst_connector_status = "1"
        |       OR cst_internal_temp_status = "1"
        |       OR cst_power_status = "1"
        |       OR cst_emergency_stop_status = "1"
        |       OR cst_current_status = "1"
        |       OR cst_volt_status = "1"
        |       OR cst_other_status = "1"
        |       OR bem_error IS NOT NULL
        |       OR cem_error IS NOT NULL) """.stripMargin
    val field_sql = " order_id,abs_time,device_id,bsm_single_volt_status,bsm_soc_status,bsm_current_status,bsm_charge_status,bsm_temp_status,bsm_insulate_status,bsm_connect_status,bst_insulation_status,bst_conn_over_temp_status,bst_bms_over_temp_status,bst_charge_conn_status,bst_battery_pack_temp_status,bst_high_volt_relay_status,bst_charging_port_volt_status,bst_current_status,bst_volt_status,bst_other_status,cst_temp_status,cst_connector_status,cst_internal_temp_status,cst_power_status,cst_emergency_stop_status,cst_current_status,cst_volt_status,cst_other_status,bem_error,cem_error "
    val sql: String = s"select $field_sql from dwd.dwd_wcd_pile_data_detail_dupli_di where dt>='${startDay}' and dt<='${endDay}' and device_id in (select model_id from dim.dim_wcd_power_model_detail_ci where dt='20240720' and station_id='100100001') and $warn_sql "
    //    val sql: String = s"select $field_sql from dwd.dwd_wcd_pile_data_detail_dupli_di where dt='20240719' and order_id='5103002620240719081559000' and $warn_sql "
    println("sql:" + sql)
    val data: DataFrame = spark.sql(sql)
    val value: RDD[Row] = data.rdd.flatMap(WarnMessage.catchWarnMessage)
      .map((x: Map[String, String]) => Row(x("orderId"), x("warnTime").toLong, x("warnName"), x("warnLevel"), x("device_id")))
    val schema: StructType = StructType(List(
      StructField("orderId", StringType, nullable = false),
      StructField("warnTime", LongType, nullable = false),
      StructField("warnName", StringType, nullable = false),
      StructField("warnLevel", StringType, nullable = false),
      StructField("deviceId", StringType, nullable = false)))
    spark.createDataFrame(value, schema).createTempView("warn_data")

    val processSql: String =
      f"""
         |with t1 as (
         |select
         |   orderId,
         |   warnName,
         |   warnTime,
         |   deviceId,
         |   lag(warnTime,1,warnTime) over (partition by orderId,warnName order by warnTime ) as lag_time
         |from warn_data ),
         | t2 as (
         | select
         | deviceId,
         | sum(if(warnTime-lag_time>${timeDiff},1,0)) over (partition by warnName,orderId order by warnTime) as flag_warn,
         | lag_time,
         | orderId,
         | warnName,
         | warnTime
         | from t1 ),
         | t3 as (select orderId,warnName,flag_warn, max(lag_time) warnTime,max(deviceId) as deviceId, count(1) as flag_cnt from t2  group by orderId,warnName,flag_warn having flag_cnt >=${dataSize} )
         | select orderId,deviceId,warnName,warnTime from t3
         |""".stripMargin

    println(processSql)

    val warnData: DataFrame = spark.sql(processSql)

    // 定义 UDF
    val mapUDF: UserDefinedFunction = udf((name: String) => levelMap.getOrElse(name, "1"))
    val dateUDF: UserDefinedFunction = udf((name: Long) => timestamp2Time(name))


    warnData
      .withColumn("warnLevel", mapUDF(col("warnName")))
      .withColumn("warnTime", dateUDF(col("warnTime")))
      .coalesce(1)
      .write.mode("overwrite").option("header", value = true).csv(path)
  }

}
