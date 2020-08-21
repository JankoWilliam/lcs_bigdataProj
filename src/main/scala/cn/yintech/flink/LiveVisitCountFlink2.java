package cn.yintech.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.Properties;

public class LiveVisitCountFlink2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "bigdata002.sj.com:9092,bigdata003.sj.com:9092,bigdata004.sj.com:9092");
        properties.setProperty("zookeeper.connect", "bigdata002.sj.com:2181,bigdata003.sj.com:2181,bigdata004.sj.com:2181");
//        properties.setProperty("auto.offset.reset", "latest");
//        properties.setProperty("enable.auto.commit", "false");
//        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("group.id", "LiveVisitCountFlink02");
        String topic = "sc_md";


        tableEnv.connect(
                new Kafka()
                        .version("0.10")
                        .topic(topic)
                        .property("zookeeper.connect", "bigdata002.sj.com:2181,bigdata003.sj.com:2181,bigdata004.sj.com:2181")
                        .property("bootstrap.servers", "bigdata002.sj.com:9092,bigdata003.sj.com:9092,bigdata004.sj.com:9092")
                        .property("group.id", "LiveVisitCountFlink02")
                        .startFromLatest()

        ).withFormat(
                new Json()
                        .failOnMissingField(true)
                        .deriveSchema()
        ).withSchema(
                new Schema()
                        .field("user_id", Types.LONG()) //一层嵌套json
                        .field("event", Types.STRING())
//                        .field("properties", ObjectArrayTypeInfo.getInfoFor(
//                                Row[].class,
//                                Types.ROW(
//                                        new String[]{"userID", "v1_message_title"},
//                                        new TypeInformation[] {Types.STRING(), Types.STRING()}
//                                )
//                        ))
        )
                .inAppendMode()
                .registerTableSource("eventlog");

        String sql = "select * from eventlog";
        Table table = tableEnv.sqlQuery(sql);
        DataStream<Row> dataStream = tableEnv.toAppendStream(table, Row.class);
        table.printSchema();
        dataStream.print();


        tableEnv.execute("LiveVisitCountFlink2");

    }

}
