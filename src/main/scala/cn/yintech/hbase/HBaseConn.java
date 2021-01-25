package cn.yintech.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseConn {
    private static final HBaseConn INSTANCE = new HBaseConn();
    private static  Configuration configuration; //hbase配置
    private static  Connection connection; //hbase connection

    public static void main(String[] args) {
        try( Table table = HBaseConn.getTable("base_event_log_test2")){
            Get get = new Get(Bytes.toBytes("E||1607875188314"));
            Result result = table.get(get);
            System.out.println("rowkey == "+Bytes.toString(result.getRow()));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private HBaseConn(){
        try{
            if (configuration==null){
                 configuration = HBaseConfiguration.create();
                 configuration.set("hbase.zookeeper.quorum","bigdata002.sj.com:2181");
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }
    private  Connection getConnection(){
        if (connection==null || connection.isClosed()){
            try{
                connection = ConnectionFactory.createConnection(configuration);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return connection;
    }
    public static Connection getHBaseConn(){
        return INSTANCE.getConnection();
    }
    public static Table getTable(String tableName) throws IOException {
        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }
    public static void closeConn(){
        if (connection!=null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}

