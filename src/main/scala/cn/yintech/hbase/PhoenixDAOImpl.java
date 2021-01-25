package cn.yintech.hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;


/**
 * @author ZLH
 * @time:2017年3月10日
 */

public class PhoenixDAOImpl {


    Connection conn = null;

    Statement stmt = null;


    public static void main(String[] args) {

        new PhoenixDAOImpl().upsertTable();
// new PhoenixDAOImpl().upsertBatch();
//        new PhoenixDAOImpl().queryAll();

    }

    /**
     * 获取连接
     *
     * @return
     */

    public Connection getConnection() {

        String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
        String url = "jdbc:phoenix:59.110.168.230:2181";
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(url);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }


    /**
     * 插入数据
     */

    public void upsertTable() {
        conn = getConnection();
        try {
            stmt = conn.createStatement();
//            for (int i = 0; i < 20000; i++) {
//                String rowkey = UUID.randomUUID().toString().replaceAll("-", "");
//                String sql = "upsert into ZLHTEST_EHRMAIN values('"
//                        + rowkey
//                        + "'," + 2222222 + i + ",'dddd" + i + "dddd','sssss','aqqqqqa','ccccccccc','sssssssss')";
//                stmt.executeUpdate(sql);
//                conn.commit();
//                System.out.println("第" + i + "条插入成功");
//            }

            String sql = "upsert into \"base_event_log_test3\" values('z','x','c','v','b','n')";
            stmt.executeUpdate(sql);
            conn.commit();
            System.out.println("插入成功");

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                stmt.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 批量插入
     */

    public void upsertBatch() {

        PreparedStatement pstmt = null;
        conn = getConnection();
        long start = System.currentTimeMillis();
        try {
            conn.setAutoCommit(false);
            String sql = "upsert into ZLHTEST_EHRMAIN values(?,?,?,?,?,?,?)";
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < 20000; i++) {
                String rowkey = UUID.randomUUID().toString().replaceAll("-", "");
                pstmt.setString(1, rowkey + i);
                pstmt.setLong(2, 33 + i);
                pstmt.setString(3, "asfsdffds");
                pstmt.setString(4, "sdfggg");
                pstmt.setString(5, "sdfff");
                pstmt.setString(6, "safgfg");
                pstmt.setString(7, "sfdghjjjj");
                if (i % 1000 == 0) {
                    pstmt.executeBatch();
                }

            }

            pstmt.executeBatch();
            conn.commit();
            long end = System.currentTimeMillis();
            long tm = end - start;
            System.out.println("总共使用时间" + tm);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                pstmt.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }

    }

    public void queryAll() {
        PreparedStatement pstmt = null;
        conn = getConnection();
        long start = System.currentTimeMillis();
        try {
            conn.setAutoCommit(false);
            String sql = "select * from ZLHTEST_EHRMAIN limit 5";
            pstmt = conn.prepareStatement(sql);
            ResultSet rset = pstmt.executeQuery(sql);
            while (rset.next()) {
                System.out.println(rset.getString(1) + " " + rset.getString(2) + " " + rset.getString(3) + " " + rset.getString(4) + " " + rset.getString(5) + " " + rset.getString(6) + " " + rset.getString(7));
            }
            long end = System.currentTimeMillis();
            long tm = end - start;
            System.out.println("总共使用时间" + tm);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                pstmt.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }

    }


}