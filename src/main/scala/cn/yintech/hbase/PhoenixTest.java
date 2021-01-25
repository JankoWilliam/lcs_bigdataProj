package cn.yintech.hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
 
public class PhoenixTest {
    /**
     * 使用phoenix提供的api操作hbase读取数据
     */
    public static void main(String[] args) throws Throwable {
        try {
            // 下面的驱动为Phoenix老版本使用2.11使用，对应hbase0.94+
            // Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");
 
            // phoenix4.3用下面的驱动对应hbase0.98+
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 这里配置zookeeper的地址，可单个，也可多个。可以是域名或者ip
        String url = "jdbc:phoenix:bigdata002.sj.com:2181";
        // String url =
        // "jdbc:phoenix:41.byzoro.com,42.byzoro.com,43.byzoro.com:2181";
        Connection conn = DriverManager.getConnection(url);
        Statement statement = conn.createStatement();
        String sql = "select \"userid\",\"event\",\"properties\" from \"base_event_log_test2\" where \"userid\" = '27556080' and \"event\" = 'NativeAppClick';";
        long time = System.currentTimeMillis();
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
            int count = rs.getInt("num");
            System.out.println("row count is " + count);
        }
        long timeUsed = System.currentTimeMillis() - time;
        System.out.println("time " + timeUsed + "mm");
        // 关闭连接
        rs.close();
        statement.close();
        conn.close();
    }
}