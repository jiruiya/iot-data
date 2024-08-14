package com.plan.data;

import com.circue.data.iot.parser.IotParser;
import com.circue.data.iot.parser.ParseConfig;
import com.circue.iot.proto.KvPairMessage;
import com.circue.iot.proto.Message.IotMessage;
import com.plan.data.source.KafkaMySource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map$;
import scala.collection.immutable.List;
import java.util.HashMap;
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

    SingleOutputStreamOperator<KvPairMessage.IotKvPair> map =
        kafkaSource
            .map((MapFunction<byte[], IotMessage>) IotMessage::parseFrom)
                    .flatMap(new FlatMapFunction<IotMessage, KvPairMessage.IotKvPair>() {
                      @Override
                      public void flatMap(IotMessage iotMessage, Collector<KvPairMessage.IotKvPair> collector) throws Exception {
                        HashMap<String, List<ParseConfig>> stringListHashMap = new HashMap<>();
                      scala.collection.mutable.Map scalaMap = JavaConversions.mapAsScalaMap(stringListHashMap);
                      Object obj = Map$.MODULE$.<String, List<ParseConfig>>newBuilder().$plus$plus$eq(scalaMap.toSeq());
                      Object result = ((scala.collection.mutable.Builder) obj).result();
                      scala.collection.immutable.Map<String, List<ParseConfig>> scala_imMap = (scala.collection.immutable.Map)result;

                      Iterator<IotMessage> iotMessageIterator =
                              JavaConverters.asScalaIteratorConverter(Stream.of(iotMessage).iterator()).asScala();
                      try{
                      Iterator<KvPairMessage.IotKvPair> parse = IotParser.parse(iotMessageIterator, scala_imMap);
                        java.util.Iterator<KvPairMessage.IotKvPair> iotKvPairIterator = JavaConversions.asJavaIterator(parse);
                        while (iotKvPairIterator.hasNext()){
                          collector.collect(iotKvPairIterator.next());
                        }
                      }catch ( Exception e){
                        System.out.println(e);
                      }
                      }
                    });
//
    env.setParallelism(2);
    map.print();
    env.execute("1");
  }
}
