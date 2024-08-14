package com.plan.data.process;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class HandelResult implements Serializable {

  /**
   * 聚合告警数据
   *
   * @param value 明细数据
   * @return 告警数据
   */
  public HashMap<String, String> handelWarnResult(Map<String, String> value) {
    HashMap<String, String> res = new HashMap<>();
    String stationId = value.get("stationId");
    String timestamp = value.get("timestamp");
    String equipmentType = value.get("equipmentType");
    String equipmentId = value.get("equipmentId");
    String terminalId = value.get("terminalId");
    String remark = value.get("remark");

    res.put("tableName", "warn_" + stationId + "_" + equipmentId + "_" + equipmentType);
    res.put("timestamp", timestamp);
    res.put("stationId", stationId);
    res.put("equipmentType", equipmentType);
    res.put("equipmentId", equipmentId);
    res.put("terminalId", terminalId);
    res.put("dataType", "warn");


      String warnLevel = "0";
      String warnName = "";

      switch (value.get("remark")) {
        case "warn":
          String fieldName = value.get("fieldName");
          int index = fieldName.lastIndexOf("_");
          String name = index == -1 ? fieldName : fieldName.substring(0, index);

          if (fieldName.equals(name + "_3") && "1".equals(value.get("fieldValue"))) {
            warnLevel = "1";
          } else if (fieldName.equals(name + "_2") && "1".equals(value.get("fieldValue"))) {
            warnLevel = "2";
          } else if (fieldName.equals(name + "_1") && "1".equals(value.get("fieldValue"))) {
            warnLevel = "3";
          }
          warnName = name;
          break;
        case "fault":
          warnLevel = value.get("fieldValue");
          warnName = value.get("fieldName");
          break;
        default:
          break;
      }
      res.put(warnName, warnLevel);
    return res;

  }

  public HashMap<String, String> handelEmResult(Map<String, String> value){

  }

  /**
   * 字段聚合
   *
   * @param value 明细数据
   * @return 聚合后的数据
   */
  public Iterator<Map<String, String>> handelAggResult(List<Map<String, String>> value) {
    // 分割 cell 数据和其他数据
    Map<Boolean, List<Map<String, String>>> partitionedData = value.stream()
            .collect(Collectors.partitioningBy(x -> "cell".equals(x.get("equipmentType"))));

    List<Map<String, String>> cellData = partitionedData.get(true);
    List<Map<String, String>> otherData = partitionedData.get(false);

    // 过滤 remark 为 "em" 的数据
    List<Map<String, String>> emData = otherData.stream()
            .filter(x -> "em".equals(x.get("remark")))
            .collect(Collectors.toList());

    // 处理 em 数据
    Iterator<Map<String, String>> emResult = emData.stream()
            .collect(Collectors.groupingBy(x -> Arrays.asList(
                    x.get("stationId"),
                    x.get("timestamp"),
                    x.get("equipmentType"),
                    x.get("equipmentId")
            )))
            .entrySet()
            .stream()
            .map(entry -> {
              List<String> keys = entry.getKey();
              String stationId = keys.get(0);
              String timestamp = keys.get(1);
              String equipmentType = keys.get(2);
              String equipmentId = keys.get(3);

              Map<String, String> res = new HashMap<>();
              res.put("tableName", "em_" + stationId + "_" + equipmentId + "_" + equipmentType);
              res.put("timestamp", timestamp);

              entry.getValue().forEach(x -> res.put(x.get("fieldName"), x.get("fieldValue")));
              return res;
            })
            .iterator();

    // 处理其他数据
    Iterator<Map<String, String>> otherResult = otherData.stream()
            .collect(Collectors.groupingBy(x -> Arrays.asList(
                    x.get("stationId"),
                    x.get("equipmentType"),
                    x.get("equipmentId"),
                    x.get("timestamp")
            )))
            .entrySet()
            .stream()
            .map(entry -> {
              List<String> keys = entry.getKey();
              String stationId = keys.get(0);
              String equipmentType = keys.get(1);
              String equipmentId = keys.get(2);
              String timestamp = keys.get(3);

              Map<String, String> res = new HashMap<>();
              res.put("tableName", equipmentType + "_" + stationId + "_" + equipmentId);
              res.put("timestamp", timestamp);
              res.put("dataType", equipmentType);

              entry.getValue().forEach(x -> res.put(x.get("fieldName"), x.get("fieldValue")));
              return res;
            })
            .iterator();

    // 处理 cell 数据
    Iterator<Map<String, String>> cellResult = cellData.stream()
            .collect(Collectors.groupingBy(x -> Arrays.asList(
                    x.get("stationId"),
                    x.get("equipmentId"),
                    x.get("timestamp")
            )))
            .entrySet()
            .stream()
            .map(entry -> {
              List<String> keys = entry.getKey();
              String stationId = keys.get(0);
              String equipmentId = keys.get(1);
              String timestamp = keys.get(2);
              String cellName = "cell_info_" + stationId + "_" + equipmentId;

              Map<String, String> res = new HashMap<>();
              res.put("tableName", cellName);
              res.put("timestamp", timestamp);
              res.put("dataType", "cell");

              entry.getValue().forEach(v -> res.put(removeTrailingNumbers(v.get("fieldName")), v.get("fieldValue")));
              return res;
            })
            .iterator();

    // 合并三个结果
//    return concatIterators(cellResult, otherResult, emResult);
  }



  /**
   * 聚合ChannelInfo数据 取场站最后一条
   *
   * @param value 明细数据
   * @return Channel数据
   */
  public Iterator<Map<String, String>> handelChannelResult(List<Map<String, String>> value) {
    return value.stream()
            .collect(Collectors.groupingBy(x -> x.get("stationId")))
            .entrySet()
            .stream()
            .map(entry -> {
              String stationId = entry.getKey();
              Map<String, String> last = entry.getValue().stream()
                      .max(Comparator.comparing(x -> x.get("timestamp")))
                      .orElse(new HashMap<>());

              Map<String, String> res = new HashMap<>();
              res.put("tableName", "channel_info_" + last.get("terminalId"));
              res.put("server_ip", last.get("serverIp"));
              res.put("server_port", last.get("serverPort"));
              res.put("client_ip", last.get("clientIp"));
              res.put("client_port", last.get("clientPort"));
              res.put("station_id", stationId);
              res.put("timestamp", last.get("timestamp"));
              res.put("dataType", "channel_info");
              return res;
            })
            .iterator();
  }

  private String removeTrailingNumbers(String s) {
    try {
      return s.replaceAll("\\d+$", "");
    } catch (Throwable t) {
      return "";
    }
  }

}

