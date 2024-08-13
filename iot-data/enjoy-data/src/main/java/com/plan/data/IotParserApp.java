package com.plan.data;

import com.circue.iot.proto.Message.IotMessage;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.Arrays;

public class IotParserApp {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
            .setBootstrapServers("datahouse-node1:9092,datahouse-node2:9092,datahouse-node3:9092")
            .setTopics("iot-message-all")
            .setGroupId("my-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new DeserializationSchema<byte[]>() {
              @Override
              public byte[] deserialize(byte[] bytes) throws IOException {
                return bytes;
              }

              @Override
              public boolean isEndOfStream(byte[] aByte) {
                return false;
              }

              @Override
              public TypeInformation<byte[]> getProducedType() {
                return null;
              }
            })
            .build();

    TypeInformation<byte[]> byteArrayTypeInfo = (TypeInformation<byte[]>) Types.PRIMITIVE_ARRAY(Types.BYTE);

    DataStreamSource<byte[]> kafkaSource = env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka source",byteArrayTypeInfo);
    SingleOutputStreamOperator<String> map = kafkaSource
            .map((MapFunction<byte[], IotMessage>) IotMessage::parseFrom)
            .map((MapFunction<IotMessage, String>) iotMessage -> iotMessage.getChannel().getServerIp());
    env.setParallelism(2);
    map.print();
    env.execute("1");
  }
}
