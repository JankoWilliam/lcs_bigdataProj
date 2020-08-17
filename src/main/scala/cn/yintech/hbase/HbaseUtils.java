package cn.yintech.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;

public class HbaseUtils {
    private final static Logger log = LoggerFactory.getLogger(HbaseUtils.class);
    public static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.199.33.12:2181");
    }

    public static void main(String[] args) throws Exception {

        String tableName = "event_log_test";
        List<String> familys = new ArrayList<>();
        familys.add("cf1");

//        new HbaseUtils().createTableBySplitKeys(tableName,familys);

        System.out.println(Arrays.deepToString(new HbaseUtils().getSplitKeys()));
    }

    private byte[][] getSplitKeys() {
        String[] keys = new String[] { "10|", "20|", "30|", "40|", "50|",
                "60|", "70|", "80|", "90|" };
        byte[][] splitKeys = new byte[keys.length][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i=0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }
    /**
     * 创建预分区hbase表
     * @param tableName 表名
     * @param columnFamily 列簇
     */
    @SuppressWarnings("resource")
    public void createTableBySplitKeys(String tableName, List<String> columnFamily) {
        try {
            if (StringUtils.isBlank(tableName) || columnFamily == null) {
                log.error("===Parameters tableName|columnFamily should not be null,Please check!===");
            }
            Connection connection = ConnectionFactory.createConnection(conf);
            TableName table = TableName.valueOf(tableName);
            Admin admin = connection.getAdmin();
            if (admin.tableExists(table)) {

            } else {
                HTableDescriptor tableDescriptor = new HTableDescriptor(
                        table);
                for (String cf : columnFamily) {
                    tableDescriptor.addFamily(new HColumnDescriptor(cf));
                }
                byte[][] splitKeys = getSplitKeys();
                admin.createTable(tableDescriptor,splitKeys);//指定splitkeys
                log.info("===Create Table " + tableName
                        + " Success!columnFamily:" + columnFamily.toString()
                        + "===");
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            log.error(String.valueOf(e));
        }
    }

}
