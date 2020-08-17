package cn.yintech.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

public class FlinkSocketTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000);
        env.getConfig().setAutoWatermarkInterval(1000);

        final SingleOutputStreamOperator<String> dataStreamSource = env.socketTextStream("192.168.19.123", 9999).assignTimestampsAndWatermarks(
                new AssignerWithPeriodicWatermarks<String>() {

                    long maxOutOfOrderness = 1000L; // 3.5 seconds
                    long currentMaxTimestamp = 0L;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }

                    @Override
                    public long extractTimestamp(String s, long l) {
                        long timestamp = Long.parseLong(s.split(",")[1]);
                        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                        return timestamp;
                    }
                }
        );

        final SingleOutputStreamOperator<Tuple2<String, String>> result = dataStreamSource.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                return new Tuple2<>(s.split(",")[0], s.split(",")[1]);
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(5))
                .apply(new WindowFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, String>> iterable, Collector<Tuple2<String, String>> collector) throws Exception {
                        String key = "";
                        String value = "";
                        for (Tuple2<String, String> v : iterable) {
                            key = v.f0;
                            value += (v.f1 + "|");
                        }

                        collector.collect(new Tuple2<>(key, value));
                    }
                });


        result.print("aaa");
        env.execute("Flink socket test");
    }

}
