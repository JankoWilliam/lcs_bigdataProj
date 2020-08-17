package cn.yintech.test;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import scala.Tuple4;

import java.text.SimpleDateFormat;
import java.util.*;

public class ReadingFromKafkaJava {

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "bigdata002.sj.com:9092,bigdata003.sj.com:9092,bigdata004.sj.com:9092");
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("group.id", "kafka_flink_test06");

        String topic = "sc_md";
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        myConsumer.setStartFromLatest(); //从最新的数据开始
        final SingleOutputStreamOperator<String> stream = env.addSource(myConsumer).assignTimestampsAndWatermarks(new TimeStampExtractor());
        final SingleOutputStreamOperator<Tuple4<String, String, String, Integer>> value = stream.map(
                new MapFunction<String, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> map(String record) throws Exception {
                        Map<String, String> dataMap = jsonParse(record);
                        return new Tuple4<String, String, String, String>(dataMap.getOrDefault("event", ""), dataMap.getOrDefault("properties", ""), dataMap.getOrDefault("time", ""), dataMap.getOrDefault("distinct_id", ""));
                    }
                })
                .filter(v -> v._1().equals("LiveVisit"))
                .map(
                        new MapFunction<Tuple4<String, String, String, String>, Tuple5<String, String, String, String, String>>() {
                            @Override
                            public Tuple5<String, String, String, String, String> map(Tuple4<String, String, String, String> v) throws Exception {
                                Map<String, String> dataMap2 = jsonParse(v._2());
                                return new Tuple5<String, String, String, String, String>(
                                        dataMap2.getOrDefault("v1_lcs_id", ""),
                                        dataMap2.getOrDefault("v1_element_content", ""),
                                        dataMap2.getOrDefault("v1_message_title", ""),
                                        v._4(),
                                        v._3());
                            }

                        })
                .filter(v -> v.f1.equals("视频直播播放") && !v.f0.equals("") && !v.f3.equals(""))
                .map(new MapFunction<Tuple5<String, String, String, String, String>, Tuple2<Tuple2<String, String>, Tuple2<String, String>>>() {
                    @Override
                    public Tuple2<Tuple2<String, String>, Tuple2<String, String>> map(Tuple5<String, String, String, String, String> v) throws Exception {
                        return new Tuple2<Tuple2<String, String>, Tuple2<String, String>>(new Tuple2<String, String>(v.f0, v.f2), new Tuple2<String, String>(v.f3, v.f4));
                    }
                }) //((lcs_id,标题),(设备id,时间))
                .keyBy(new KeySelector<Tuple2<Tuple2<String, String>, Tuple2<String, String>>, String>() {
                    @Override
                    public String getKey(Tuple2<Tuple2<String, String>, Tuple2<String, String>> v) throws Exception {
                        return (String)v.f0.f0 + (String)v.f0.f1;
                    }
                })
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .apply(new WindowFunction<Tuple2<Tuple2<String, String>, Tuple2<String, String>>, Tuple4<String, String, String, Integer>, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow timeWindow, Iterable<Tuple2<Tuple2<String, String>, Tuple2<String, String>>> input, Collector<Tuple4<String, String, String, Integer>> out) throws Exception {
                                Set<String> set = new HashSet();
                                List<Tuple2<Tuple2<String, String>, Tuple2<String, String>>> inputList = IteratorUtils.toList(input.iterator());
                                final Tuple2<Tuple2<String, String>, Tuple2<String, String>> max = Collections.max(inputList, new Comparator<Tuple2<Tuple2<String, String>, Tuple2<String, String>>>() {
                                    @Override
                                    public int compare(Tuple2<Tuple2<String, String>, Tuple2<String, String>> o1, Tuple2<Tuple2<String, String>, Tuple2<String, String>> o2) {
                                        return ((String) o1.f1.f1).compareTo(((String) o2.f1.f1));
                                    }
                                });
                                input.forEach(v -> {
                                    set.add((String) v.f1.f1);
                                });
                                out.collect(new Tuple4<String, String, String, Integer>((String) max.f0.f0, (String) max.f0.f1, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Long.parseLong(max.f1.f1)), set.size()));
                            }
                        });
        value.print();

        env.execute("Flink java Streaming");


    }


    static class TimeStampExtractor implements AssignerWithPeriodicWatermarks<String> {

        Long maxOutOfOrderness = 30000L; // 3.5 seconds
        Long currentMaxTimestamp = 0L;
        Watermark a = null;

        @Override
        public Watermark getCurrentWatermark() {
            a = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            return  a;
        }

        @Override
        public long extractTimestamp(String s, long l) {

            long timestamp = Long.parseLong(jsonParse(s).getOrDefault("time", null));
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            System.out.println("timestamp:" + sdf.format(timestamp) + "|currentMaxTimestamp:" + sdf.format(currentMaxTimestamp) + "|Watermark:" + a.toString() + "--" + sdf.format(a.getTimestamp()) + "|systime:" + sdf.format(new Date()));
            return timestamp;
        }

    }

    public static Map<String, String> jsonParse(String value) {
        Map<String, String> map = new HashMap<>();
        JSONParser jsonParser = new JSONParser();
        try {
            JSONObject outJsonObj = (JSONObject) jsonParser.parse(value);
            Set<String> outJsonKey = outJsonObj.keySet();
            Iterator<String> outIter = outJsonKey.iterator();

            while (outIter.hasNext()) {
                String outKey = outIter.next();
                String outValue = (outJsonObj.get(outKey) != null) ? outJsonObj.get(outKey).toString() : "null";
                map.put(outKey, outValue);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return map;
    }
}
