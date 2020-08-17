package cn.yintech.eventLog;

import com.alibaba.fastjson.JSONObject;
import org.apache.lucene.util.automaton.LimitedFiniteStringsIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class EventLogSchema {

    public static void main(String[] args) {

        List schema = new ArrayList<String>();
        String jsonStr = "{\"_track_id\":-316466639,\"time\":1582646381864,\"type\":\"track\",\"distinct_id\":\"48210eefd2e7854b\",\"lib\":{\"$lib\":\"Android\",\"$lib_version\":\"3.2.8\",\"$app_version\":\"4.3.17\",\"$lib_method\":\"code\",\"$lib_detail\":\"com.sensorsdata.analytics.android.sdk.SensorsDataAPI##trackEvent##SensorsDataAPI.java##104\"},\"event\":\"NativeAppClick\",\"properties\":{\"$lib\":\"Android\",\"$carrier\":\"中国移动\",\"$os_version\":\"9\",\"$device_id\":\"48210eefd2e7854b\",\"$lib_version\":\"3.2.8\",\"$model\":\"SEA-AL10\",\"$os\":\"Android\",\"$screen_width\":1080,\"$screen_height\":2340,\"$manufacturer\":\"HUAWEI\",\"$app_version\":\"4.3.17\",\"ABTest\":\"discovery\",\"userID\":\"24465474\",\"deviceId\":\"862941031786454\",\"PushStatus\":false,\"$utm_source\":\"huaweipay\",\"$utm_campaign\":\"huaweipay\",\"stock_num\":74,\"planner_num\":\"4\",\"$wifi\":true,\"$network_type\":\"WIFI\",\"v1_element_content\":\"视频_唤起进度条\",\"$is_first_day\":false,\"$ip\":\"171.217.98.108\",\"$is_login_id\":false,\"$city\":\"成都\",\"$province\":\"四川\",\"$country\":\"中国\"},\"_flush_time\":1582646383141,\"map_id\":\"48210eefd2e7854b\",\"user_id\":2224651458783474868,\"recv_time\":1582646388072,\"extractor\":{\"f\":\"(dev=863,ino=9700590)\",\"o\":598249299,\"n\":\"access_log.2020022523\",\"s\":5865398431,\"c\":5865398439,\"e\":\"data02.yinke.sa\"},\"project_id\":39,\"project\":\"licaishi\",\"ver\":2}";


        //先将这条数据解析为JSONObject
        JSONObject outJson = JSONObject.parseObject(jsonStr);
        //因为外部的JSON的key为三位数字的编号，我们需要得到编号，才能得到它对应的内部json
        Set<String> jsonSet = outJson.keySet();
        Iterator<String> iterator = jsonSet.iterator();
        while (iterator.hasNext()){
            //通过迭代器可以取到外部json的key
            String key1 = iterator.next();
            //取得内部json字符串
            String value1 = outJson.getString(key1);
            System.out.println(key1);
            if (key1.startsWith("_")) key1 = "tmp" + key1;
            //将内部json字符串解析为object对象
//            JSONObject inJson = JSONObject.parseObject(string);
            if ( JSONObject.isValidObject(value1)){
                JSONObject innerJson = JSONObject.parseObject(value1);
                Set<String> innerJsonSet = innerJson.keySet();
                Iterator<String> innerIterator = innerJsonSet.iterator();

                while (innerIterator.hasNext()){
                    String key2 = innerIterator.next();
                    System.out.println("\t"+key2.replace("$",""));
                    String value2 = innerJson.getString(key2);
                    schema.add((key1+"_"+key2).replace("$",""));
                }
            } else {
                schema.add(key1);
            }

        }

        System.out.println(schema);
        System.out.println(schema.size());

        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        for (int i = 0; i < schema.size(); i++) {
            sb1.append(schema.get(i)).append("\n");
            sb2.append("dataMap.getOrElse(\"" + schema.get(i) + "\",\"\"),\n");
        }
//        System.out.println("sb1 : " + sb1.toString());
//        System.out.println("sb2 : " + sb2.toString());

    }


}