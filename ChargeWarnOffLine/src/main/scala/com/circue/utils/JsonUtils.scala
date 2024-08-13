package com.circue.utils

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import java.io.IOException
//import java.util


object JsonUtils {

  private val objectMapper: ObjectMapper = new ObjectMapper
  objectMapper.registerModule(DefaultScalaModule)


  // 将JSON字符串转换为Map
  @throws[IOException]
  def jsonToMap(jsonString: String): Map[String, String] =
    try {
      val a: Map[String, Any] = objectMapper.readValue(jsonString,classOf[Map[String,Any]])
      a.map {
        case (key, value: String) => (key, value)
        case (key, value) => (key, value.toString)
      }
    }
    catch {
      case _:Exception=>
        Map[String, String]()
    }

  // 将Map转换为JSON字符串
  @throws[JsonProcessingException]
  def mapToJson(map: Map[String, Any]): String =
    try objectMapper.writeValueAsString(map)
    catch {
      case _:Exception => ""
    }

  @throws[JsonProcessingException]
  def jsonToList(jsonString: String): List[Map[String, Any]] = {
    try objectMapper.readValue(jsonString, classOf[List[Map[String, Any]]])
    catch {
      case _: Exception =>
        List[Map[String, Any]]()
    }
  }

}







