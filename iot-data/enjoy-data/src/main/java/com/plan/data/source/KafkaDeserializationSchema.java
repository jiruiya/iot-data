package com.plan.data.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class KafkaDeserializationSchema implements DeserializationSchema<byte[]> {
  @Override
  public byte[] deserialize(byte[] bytes) throws IOException {
    return bytes;
  }

  @Override
  public boolean isEndOfStream(byte[] bytes) {
    return false;
  }

  @Override
  public TypeInformation<byte[]> getProducedType() {
    return null;
  }
}
