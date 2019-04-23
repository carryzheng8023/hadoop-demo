package xin.carryzheng.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhengxin
 * @date 2019-04-04 09:40:40
 */
public class HBaseDemo {

    private static Configuration conf = HBaseConfiguration.create();

    static {
        conf.set("hbase.rootdir", "hdfs://hadoop01:9000/hbase");
        conf.set("hbase.zookeeper.quorum", "hadoop01:2181");
    }


    /**
     * 判断表是否存在
     *
     * @author zhengxin
     * @date   2019/4/4 09:55
     */
    public static boolean isExist(String tableName) throws IOException {

        //老API
//        HBaseAdmin admin = new HBaseAdmin(conf);

        //新API
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        return admin.isTableAvailable(TableName.valueOf(tableName));

    }


    /**
     * 创建表
     *
     * @author zhengxin
     * @date   2019/4/4 09:56
     */
    public static void createTable(String tableName, String... columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        if(isExist(tableName)){
            System.out.println(tableName + "表已存在！");
            return;
        }

        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : columnFamily){
            htd.addFamily(new HColumnDescriptor(cf));
        }

        admin.createTable(htd);
        System.out.println(tableName + "表创建成功！");

    }


    /**
     * 删除表
     *
     * @author zhengxin
     * @date   2019/4/4 10:06
     */
    public static void deleteTable(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        TableName tn = TableName.valueOf(tableName);

        if(isExist(tableName)){
            if(!admin.isTableDisabled(tn)){
                admin.disableTable(tn);
            }
            admin.deleteTable(tn);
            System.out.println(tableName + "表删除成功！");
        }else {
            System.out.println(tableName + "表不存在！");
        }

    }

    /**
     * 添加一行数据
     *
     * @author zhengxin
     * @date   2019/4/4 10:16
     */
    public static void addRow(String tableName, String rowKey, String cf, String col, String value) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col), Bytes.toBytes(value));
        table.put(put);

    }

    /**
     * 删除一行数据
     *
     * @author zhengxin
     * @date   2019/4/4 10:26
     */
    public static void deleteRow(String tableName, String rowKey, String cf) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(Bytes.toBytes(rowKey));

        table.delete(delete);
    }

    public static void deleteMultiRow(String tableName, String... rowKeys) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);

        Table table = connection.getTable(TableName.valueOf(tableName));

        List<Delete> list = new ArrayList<>();

        for(String rowKey : rowKeys){
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            list.add(delete);
        }

        table.delete(list);
    }

    /**
     * 扫描数据
     *
     * @author zhengxin
     * @date   2019/4/4 10:35
     */
    public static void getAllRows(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
//        scan.setMaxVersions();

        ResultScanner rs = table.getScanner(scan);
        for(Result result : rs){
//            System.out.println(Bytes.toString(result.getRow()));
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                System.out.println("行键：" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列名：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值：" + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println();
            }
        }

    }


    /**
     * 得到一个具体的数据
     *
     * @author zhengxin
     * @date   2019/4/4 10:51
     */
    public static void getRow(String tableName, String rowKey) throws IOException {

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
//        get.addFamily();
//        get.addColumn();

        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for(Cell cell : cells){
            System.out.println("行键：" + Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列名：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值：" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println();
        }
    }


    public static void main(String[] args) throws IOException {

//        System.out.println(isExist("zx:student"));
//        createTable("staff", "info1", "info2");
//        deleteTable("staff");
//        addRow("staff", "1001", "info1", "name", "zhangsan");
//        addRow("staff", "1001", "info1", "age", "18");
//        addRow("staff", "1002", "info1", "name", "lisi");
//        addRow("staff", "1003", "info1", "name", "wangwu");
//        deleteRow("staff", "1001", null);

//        deleteMultiRow("staff", "1001", "1002", "1003");

//        getAllRows("staff");

        getRow("staff", "1001");
    }
}
