package com.plan.data;

import com.circue.data.iot.parser.IotParser;
import com.circue.iot.proto.Message.IotMessage;
import com.plan.data.source.KafkaDeserializationSchema;
import com.plan.data.source.KafkaMySource;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class IotParserApp {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    TypeInformation<byte[]> byteArrayTypeInfo =
        (TypeInformation<byte[]>) Types.PRIMITIVE_ARRAY(Types.BYTE);

    DataStreamSource<byte[]> kafkaSource =
        env.fromSource(
            KafkaMySource.getKafkaSource(),
            WatermarkStrategy.noWatermarks(),
            "kafka source",
            byteArrayTypeInfo);

    SingleOutputStreamOperator<String> map =
        kafkaSource
            .map((MapFunction<byte[], IotMessage>) IotMessage::parseFrom)
            .map(new MapFunction<IotMessage, String>() {
                @Override
                public String map(IotMessage iotMessage) throws Exception {
                    HashMap<String, List> stringListHashMap = new HashMap<>();
                    IotParser.parse(Stream.of(iotMessage).iterator().asScala(),stringListHashMap );
                }
            })
    env.setParallelism(2);
    map.print();
    env.execute("1");
  }
}
