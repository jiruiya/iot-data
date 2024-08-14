package com.plan.data.process;

import com.circue.iot.proto.KvPairMessage;
import com.circue.iot.proto.Message;

import java.util.HashMap;

public class ParserKvPair {

  /**
   * 获取kvPair的属性
   *
   * @param v IotKvPair
   * @return 属性
   */
  public static HashMap<String, String> getKvInfo(KvPairMessage.IotKvPair v) {
    KvPairMessage.IotEquipInfo iotEquipInfo = v.getEquipInfo();
    Message.ChannelInfo channel = v.getChannel();
    KvPairMessage.ValueKind kind = v.getValueKind();
    int fieldTypeMap = kind.getNumber();
    String fieldValue = null;

    switch (fieldTypeMap) {
      case 1:
        fieldValue = String.valueOf(v.getAnalog());
        break;
      case 2:
        fieldValue = String.valueOf(v.getDigital());
        break;
      case 3:
        fieldValue = v.getLiteral();
        break;
      default:
        break;
    }

    HashMap<String, String> map = new HashMap<>();
    map.put("stationId", iotEquipInfo.getStationId());
    map.put("equipmentType", iotEquipInfo.getEquipmentType());
    map.put("equipmentId", iotEquipInfo.getEquipmentId());
    map.put("timestamp", String.valueOf(v.getTimestamp()));
    map.put("fieldName", v.getPropertyName());
    map.put("fieldValue", fieldValue);
    map.put("serverIp", channel.getServerIp());
    map.put("serverPort", String.valueOf(channel.getServerPort()));
    map.put("clientIp", channel.getClientIp());
    map.put("clientPort", String.valueOf(channel.getClientPort()));
    map.put("terminalId", iotEquipInfo.getTerminalId());
    map.put("remark", v.getRemark());
    map.put("cab", iotEquipInfo.getCab());
    map.put("stack", iotEquipInfo.getStack());
    map.put("cluster", iotEquipInfo.getCluster());
    map.put("pack", iotEquipInfo.getPack());
    map.put("cell", iotEquipInfo.getCell());

    return map;
  }
}
