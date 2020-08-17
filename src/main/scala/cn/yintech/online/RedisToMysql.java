package cn.yintech.online;

import cn.yintech.redisUtil.RedisClientJava;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import redis.clients.jedis.Jedis;

import java.sql.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


public class RedisToMysql {

    public static void main(String[] args) {

        final Jedis jedis = RedisClientJava.getJedisPoolInstance().getResource();
        final Map<String, String> liveVisitCount = jedis.hgetAll("lcs:live:visit:count");
//        liveVisitCount.forEach( (k,v)-> System.out.println(k + ":" + v));

        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
//        conn =  DriverManager.getConnection("jdbc:mysql://localhost/test?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "root", "root");
            conn =  DriverManager.getConnection("jdbc:mysql://192.168.195.212/finebi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "finebi", "@Lcs201707");
            String sql = "insert into lcs_live_visit_count values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
            final PreparedStatement preparedStatement = conn.prepareStatement(sql);
            conn.setAutoCommit(false);
            liveVisitCount.forEach( (k,v)-> {
                final Map<String, String> data = jsonParse(v);
                try {
                    preparedStatement.setLong(1,Long.parseLong(data.getOrDefault("live_id","0")));
                    preparedStatement.setLong(2,Long.parseLong(data.getOrDefault("circle_id","0")));
                    preparedStatement.setLong(3,Long.parseLong(data.getOrDefault("lcs_id","0")));
                    preparedStatement.setString(4,data.getOrDefault("lcs_name",""));
                    preparedStatement.setString(5,data.getOrDefault("start_time",""));
                    preparedStatement.setString(6,data.getOrDefault("end_time",""));
                    preparedStatement.setString(7,data.getOrDefault("live_title",""));
                    preparedStatement.setInt(8,Integer.parseInt(data.getOrDefault("live_status","0")));
                    preparedStatement.setInt(9,Integer.parseInt(data.getOrDefault("view_count","0")));
                    preparedStatement.setInt(10,Integer.parseInt(data.getOrDefault("view_count_robot","0")));
                    preparedStatement.setInt(11,Integer.parseInt(data.getOrDefault("first_view_count","0")));
                    preparedStatement.setInt(12,Integer.parseInt(data.getOrDefault("max_living_count","0")));
                    preparedStatement.setInt(13,Integer.parseInt(data.getOrDefault("new_follow_count","0")));
                    preparedStatement.setInt(14,Integer.parseInt(data.getOrDefault("comment_count","0")));
                    preparedStatement.setInt(15,Integer.parseInt(data.getOrDefault("comment_peoples","0")));
                    preparedStatement.setInt(16,Integer.parseInt(data.getOrDefault("gift_count","0")));
                    preparedStatement.setInt(17,Integer.parseInt(data.getOrDefault("share_count","0")));
                    preparedStatement.setInt(18,Integer.parseInt(data.getOrDefault("new_follow_count_robot","0")));
                    preparedStatement.setInt(19,Integer.parseInt(data.getOrDefault("comment_count_robot","0")));
                    preparedStatement.setInt(20,Integer.parseInt(data.getOrDefault("comment_peoples_robot","0")));
                    preparedStatement.setInt(21,Integer.parseInt(data.getOrDefault("gift_count_robot","0")));
                    preparedStatement.setInt(22,Integer.parseInt(data.getOrDefault("share_count_robot","0")));
                    preparedStatement.setDouble(23, Double.valueOf(String.format("%.1f", Double.parseDouble(data.getOrDefault("old_user_staying_average", "0.0")) / 60)));
                    preparedStatement.setInt(24, Integer.parseInt(data.getOrDefault("old_user_staying_max", "0")));
                    preparedStatement.setInt(25, Integer.parseInt(data.getOrDefault("old_user_staying_min", "0")));
                    preparedStatement.setDouble(26, Double.valueOf(String.format("%.1f", Double.parseDouble(data.getOrDefault("new_user_staying_average", "0.0")) / 60)));
                    preparedStatement.setInt(27,Integer.parseInt(data.getOrDefault("new_user_staying_max","0")));
                    preparedStatement.setInt(28,Integer.parseInt(data.getOrDefault("new_user_staying_min","0")));
                    preparedStatement.setInt(29,Integer.parseInt(data.getOrDefault("live_type","0")));
                    preparedStatement.addBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
            preparedStatement.executeBatch();
            conn.commit();

            preparedStatement.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (Exception e){
                e.printStackTrace();
            }
        }

        jedis.close();

    }

    public  static  Map<String, String> jsonParse(String value)  {
        Map<String, String> map = new HashMap<>();
        final JSONParser jsonParser = new JSONParser();
        try{
            JSONObject outJsonObj = (JSONObject)jsonParser.parse(value);
            final Set<String> outJsonKey = outJsonObj.keySet();
            final Iterator<String> outIter = outJsonKey.iterator();

            while (outIter.hasNext()) {
                final String outKey = outIter.next();
                String outValue =  (outJsonObj.get(outKey) != null)? outJsonObj.get(outKey).toString(): "null";
                map.put(outKey,outValue);
            }
        } catch(Exception e) {

        }
        return map;
    }

}
