package com.plan.data.source;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class KafkaMySource {
    public static KafkaSource<byte[]> getKafkaSource(){
        return KafkaSource.<byte[]>builder()
                .setBootstrapServers("datahouse-node1:9092,datahouse-node2:9092,datahouse-node3:9092")
                .setTopics("iot-message-all")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new KafkaDeserializationSchema())
                .build();
    }

}
