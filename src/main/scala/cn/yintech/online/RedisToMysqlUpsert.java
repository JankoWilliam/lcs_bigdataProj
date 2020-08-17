package cn.yintech.online;

import cn.yintech.redisUtil.RedisClientJava;
import redis.clients.jedis.Jedis;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;


public class RedisToMysqlUpsert {

    public static void main(String[] args) throws ParseException, SQLException {

        final List<Long> liveIds = readLiveId();
        final Jedis jedis = RedisClientJava.getJedisPoolInstance().getResource();
//        final Connection conn =  DriverManager.getConnection("jdbc:mysql://localhost/test?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "root", "root");
        final Connection conn =  DriverManager.getConnection("jdbc:mysql://192.168.195.212/finebi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "finebi", "@Lcs201707");
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            Statement stmt = conn.createStatement();

            liveIds.forEach(v -> {
                final String hget = jedis.hget("lcs:live:visit:count", v.toString());
                final Map<String, String> data = RedisToMysql.jsonParse(hget);
                String sql = null;
                try {
                    ResultSet rs = stmt.executeQuery("SELECT lcs_id from lcs_live_visit_count where live_id = " + v );
                    rs.last();
                    if (rs.getRow() > 0) {
                        sql = "update lcs_live_visit_count set " +
                                " live_id=?,circle_id=?,lcs_id=?,lcs_name=?,start_time=?,end_time=?,live_title=?," +
                                " live_status=?,view_count=?,view_count_robot=?,first_view_count=?,max_living_count=?,new_follow_count=?,comment_count=?," +
                                " comment_peoples=?,gift_count=?,share_count=?,new_follow_count_robot=?,comment_count_robot=?,comment_peoples_robot=?,gift_count_robot=?," +
                                " share_count_robot=?,old_user_staying_average=?,old_user_staying_max=?,old_user_staying_min=?,new_user_staying_average=?,new_user_staying_max=?,new_user_staying_min=?,live_type=? " +
                                " where live_id = " + v.toString();
                    } else {
                        sql = "insert into lcs_live_visit_count values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
                    }
                    final PreparedStatement preparedStatement = conn.prepareStatement(sql);

                    preparedStatement.setLong(1, Long.parseLong(data.getOrDefault("live_id", "0")));
                    preparedStatement.setLong(2, Long.parseLong(data.getOrDefault("circle_id", "0")));
                    preparedStatement.setLong(3, Long.parseLong(data.getOrDefault("lcs_id", "0")));
                    preparedStatement.setString(4, data.getOrDefault("lcs_name", ""));
                    preparedStatement.setString(5, data.getOrDefault("start_time", ""));
                    preparedStatement.setString(6, data.getOrDefault("end_time", ""));
                    preparedStatement.setString(7, data.getOrDefault("live_title", ""));
                    preparedStatement.setInt(8, Integer.parseInt(data.getOrDefault("live_status", "0")));
                    preparedStatement.setInt(9, Integer.parseInt(data.getOrDefault("view_count", "0")));
                    preparedStatement.setInt(10, Integer.parseInt(data.getOrDefault("view_count_robot", "0")));
                    preparedStatement.setInt(11, Integer.parseInt(data.getOrDefault("first_view_count", "0")));
                    preparedStatement.setInt(12, Integer.parseInt(data.getOrDefault("max_living_count", "0")));
                    preparedStatement.setInt(13, Integer.parseInt(data.getOrDefault("new_follow_count", "0")));
                    preparedStatement.setInt(14, Integer.parseInt(data.getOrDefault("comment_count", "0")));
                    preparedStatement.setInt(15, Integer.parseInt(data.getOrDefault("comment_peoples", "0")));
                    preparedStatement.setInt(16, Integer.parseInt(data.getOrDefault("gift_count", "0")));
                    preparedStatement.setInt(17, Integer.parseInt(data.getOrDefault("share_count", "0")));
                    preparedStatement.setInt(18, Integer.parseInt(data.getOrDefault("new_follow_count_robot", "0")));
                    preparedStatement.setInt(19, Integer.parseInt(data.getOrDefault("comment_count_robot", "0")));
                    preparedStatement.setInt(20, Integer.parseInt(data.getOrDefault("comment_peoples_robot", "0")));
                    preparedStatement.setInt(21, Integer.parseInt(data.getOrDefault("gift_count_robot", "0")));
                    preparedStatement.setInt(22, Integer.parseInt(data.getOrDefault("share_count_robot", "0")));
                    preparedStatement.setDouble(23, Double.valueOf(String.format("%.1f", Double.parseDouble(data.getOrDefault("old_user_staying_average", "0.0")) / 60)));
                    preparedStatement.setInt(24, Integer.parseInt(data.getOrDefault("old_user_staying_max", "0")));
                    preparedStatement.setInt(25, Integer.parseInt(data.getOrDefault("old_user_staying_min", "0")));
                    preparedStatement.setDouble(26, Double.valueOf(String.format("%.1f", Double.parseDouble(data.getOrDefault("new_user_staying_average", "0.0")) / 60)));
                    preparedStatement.setInt(27, Integer.parseInt(data.getOrDefault("new_user_staying_max", "0")));
                    preparedStatement.setInt(28, Integer.parseInt(data.getOrDefault("new_user_staying_min", "0")));
                    preparedStatement.setInt(29, Integer.parseInt(data.getOrDefault("live_type", "0")));

//                    System.out.println(preparedStatement);
                    preparedStatement.execute();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        jedis.close();
    }


    private static List readLiveId() throws ParseException {

        Connection conn = null;
        Statement stmt = null;
        List<Long> result = new ArrayList<>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String startDate = sdf.format(new Date());
        final Calendar cal = Calendar.getInstance();
        cal.setTime(sdf.parse(startDate));
        cal.add(Calendar.DAY_OF_YEAR, 1);
        String endDate = sdf.format(cal.getTime());
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
//            conn = DriverManager.getConnection("jdbc:mysql://rm-2zebtm824um01072v5o.mysql.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "lcs_spider_r", "qE1$eB1*mF3}");
            conn = DriverManager.getConnection("jdbc:mysql://rm-2zebtm824um01072v.mysql.rds.aliyuncs.com/licaishi?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "lcs_spider_r", "qE1$eB1*mF3}");
            stmt = conn.createStatement();
            String sql = "SELECT id FROM lcs_circle_notice WHERE end_time BETWEEN '" + startDate + "' and '" + endDate + "' and live_status in (0,1)";
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                result.add(rs.getLong("id"));
            }
            rs.close();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null)
                    stmt.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return result;
    }
}
