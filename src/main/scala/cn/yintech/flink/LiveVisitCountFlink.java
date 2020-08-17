package cn.yintech.flink;


import com.alibaba.fastjson.JSON;
import com.sun.deploy.util.ParameterUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public class LiveVisitCountFlink {

    public static void main(String[] args) throws IOException {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(60*1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30*1000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "bigdata002.sj.com:9092,bigdata003.sj.com:9092,bigdata004.sj.com:9092");
//        properties.setProperty("auto.offset.reset", "latest");
//        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("group.id", "LiveVisitCountFlink01");
        String topic = "sc_md";


        final DataStream<String> sourceStream = env.addSource(new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                properties
        ))
                .setParallelism(3)
                .name("sourceStream")
                .uid("sourceStream01");

        final SingleOutputStreamOperator<SensorsEvent> eventStream = sourceStream.map(message -> JSON.parseObject(message, SensorsEvent.class))
                .filter(new FilterFunction<SensorsEvent>() {
                    @Override
                    public boolean filter(SensorsEvent sensorsEvent) throws Exception {
                        return sensorsEvent.getEvent().equals("LiveVisit");
                    }
                })
                .name("eventStream").uid("eventStream01");

        final WindowedStream<SensorsEvent, Tuple, TimeWindow> dayWindowStream = eventStream.keyBy("properties.v1_message_id")
                .window(TumblingProcessingTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)));

//        dayWindowStream.aggregate()


    }
    public static final class MyAggregateFunc
        implements AggregateFunction<SensorsEvent,MyAccumulator,MyAccumulator>{

        private static final long serialVersionUID = 7180051005224302796L;

        @Override
        public MyAccumulator createAccumulator() {
            return new MyAccumulator();
        }

        @Override
        public MyAccumulator add(SensorsEvent sensorsEvent, MyAccumulator myAccumulator) {
            return null;
        }

        @Override
        public MyAccumulator getResult(MyAccumulator myAccumulator) {
            return null;
        }

        @Override
        public MyAccumulator merge(MyAccumulator myAccumulator, MyAccumulator acc1) {
            return null;
        }
    }

    public static class MyAccumulator implements Serializable {

        private static final long serialVersionUID = -5100567791603673038L;

        long userNum;
        long liveNum;
        long maxLiveNum;
        long updateTime;

        public MyAccumulator() {
            this.userNum = 0L;
            this.liveNum = 0L;
            this.maxLiveNum = 0L;
            this.updateTime = 0L;
        }
    }

}
