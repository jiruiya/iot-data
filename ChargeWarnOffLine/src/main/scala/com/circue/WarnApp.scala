package com.circue

import org.apache.spark.sql.SparkSession

object WarnApp {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val spark: SparkSession = SparkSession
      .builder
      .appName("dwd_mcd_power_station_electricity_price_detail_di_%s")
      .master("local[*]")
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

    spark
  }

}
