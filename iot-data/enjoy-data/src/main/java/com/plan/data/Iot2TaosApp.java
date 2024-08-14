package com.plan.data;

import com.circue.iot.proto.KvPairMessage;
import com.plan.data.process.ParserKvPair;
import com.plan.data.source.KafkaMySource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;

public class Iot2TaosApp {
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
    SingleOutputStreamOperator<HashMap<String,String>> properName = kafkaSource
            .map((MapFunction<byte[], KvPairMessage.IotKvPair>) KvPairMessage.IotKvPair::parseFrom)
            .map((MapFunction<KvPairMessage.IotKvPair, HashMap<String,String>>) ParserKvPair::getKvInfo);

    OutputTag<HashMap<String, String>> warnDataTags = new OutputTag<>("warnData");
    OutputTag<HashMap<String, String>> detailDataTags = new OutputTag<>("detailData");


    SingleOutputStreamOperator<HashMap<String, String>> mainData = properName.process(new ProcessFunction<HashMap<String, String>, HashMap<String, String>>() {
      @Override
      public void processElement(HashMap<String, String> v, Context ctx, Collector<HashMap<String, String>> collector) throws Exception {
        String dateType = v.getOrDefault("remark", "");
        if (dateType.equalsIgnoreCase("warn") || dateType.equalsIgnoreCase("fault")) {
          ctx.output(warnDataTags, v);
        } else {
          ctx.output(detailDataTags, v);
        }
      }
    });
    // 报警数据
    SideOutputDataStream<HashMap<String, String>> warnData = mainData.getSideOutput(warnDataTags);
    // 明细数据
    SideOutputDataStream<HashMap<String, String>> detailData = mainData.getSideOutput(detailDataTags);





    properName.print();
    env.execute("iot");
  }
}
