package com.plan.data.source;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class KafkaMySource {
    public static KafkaSource<byte[]> getKafkaSource(){
        return KafkaSource.<byte[]>builder()
                .setBootstrapServers("dev-host1:9092,dev-host2:9092,dev-host3:9092")
                .setTopics("iot-kvPair")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new KafkaDeserializationSchema())
                .build();

    }

}
