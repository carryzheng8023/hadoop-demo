package xin.carryzheng.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * @author zhengxin
 * @date 2018-04-09 14:47:47
 */
public class HBaseDao {

    private static Configuration conf = HBaseConfiguration.create();

    static {

        conf.set("hbase.rootdir", "hdfs://hadoop01:9000/hbase");
        conf.set("hbase.zookeeper.quorum", "hadoop01:2181");

    }



    //创建一张表，通过Admin HTableDescriptor 来创建
    public static void createTable(Connection connection,String tableName, String[] columnFamilies)throws Exception{
        Admin admin = connection.getAdmin();
        TableName tn = TableName.valueOf(tableName);
        if(admin.tableExists(tn)){
            System.out.println("table "+ tableName + " Exists!");
        }else{
            HTableDescriptor tableDesc = new HTableDescriptor(tn);
            for (String columnFamily : columnFamilies)
                tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
            System.out.println("table " + tableName + " create success!");
        }
    }


    //添加一条数据，通过HTable Put 为已经存在的表来添加数据
    public static void put(Connection connection,String tableName,String row,String columnFamily,String qualifier, String data)throws Exception{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put p1 = new Put(Bytes.toBytes(row));
        p1.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(qualifier),Bytes.toBytes(data));
        table.put(p1);
    }

    //根据row key获取表中的该行数据
    public static Map<String, String> get(Connection connection,String tableName,String row)throws IOException {

        Map<String, String> resultMap = new HashMap<>();
        resultMap.put("id", row);

        Table table = connection.getTable(TableName.valueOf(tableName));
        Get g = new Get(Bytes.toBytes(row));
        Result result = table.get(g);
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMap = result.getMap();
        for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : navigableMap.entrySet()){
            String columnFamily = Bytes.toString(entry.getKey());
            NavigableMap<byte[], NavigableMap<Long, byte[]>> map = entry.getValue();
            gen(resultMap, map, columnFamily);

        }

        return resultMap;

    }
    //根据TableName获取整张表中的数据
    public static List<Map<String, String>> scan(Connection connection, String tableName)throws Exception{
        List<Map<String, String>> returnedList = new ArrayList<>();
        Map<String, String> resultMap = null;

        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan s = new Scan();
        ResultScanner rs = table.getScanner(s);
        //遍历表中的数据
        for(Result r : rs){
            resultMap = new HashMap<>();
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMap = r.getMap();
            for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry:navigableMap.entrySet()){
                resultMap.put("id", Bytes.toString(r.getRow()));
                String columnFamily = Bytes.toString(entry.getKey());
                NavigableMap<byte[], NavigableMap<Long, byte[]>> map = entry.getValue();
                gen(resultMap, map, columnFamily);
            }
            returnedList.add(resultMap);
        }

        return returnedList;

    }
    //删除表中的数据
    public static boolean delete(Connection connection,String tableName) throws IOException{
        TableName tn = TableName.valueOf(tableName);
        Admin admin = connection.getAdmin();
        if(admin.tableExists(tn)){
            try{
                admin.disableTable(tn);
                admin.deleteTable(tn);
            }catch(Exception ex){
                ex.printStackTrace();
                return false;
            }
        }
        return true;
    }
    public static void main(String[] args){
        String tableName = "hbase_tb";
        String base_info = "base_info";
        String extra_info = "extra_info";
        String[] columnFamilies = {base_info, extra_info};
        try(Connection connection = ConnectionFactory.createConnection(conf)){

            HBaseDao.createTable(connection,tableName,columnFamilies);
            HBaseDao.put(connection,tableName,"row1",base_info,"name","zhangsan");
            HBaseDao.put(connection,tableName,"row1",base_info,"age","18");
            HBaseDao.put(connection,tableName,"row1",extra_info,"salary","20000");
            HBaseDao.put(connection,tableName,"row2",base_info,"name","lisi");
            HBaseDao.put(connection,tableName,"row2",base_info,"gender","male");

            System.out.println(HBaseDao.get(connection,tableName,"row1").toString());
            System.out.println(HBaseDao.get(connection,tableName,"row2").toString());
            System.out.println(HBaseDao.scan(connection,tableName).toString());

            if(HBaseDao.delete(connection,tableName)){
                System.out.println("Delete table:" + tableName + " success!");
            }
        }catch(Exception e){
            e.printStackTrace();
        }

    }



    private static void gen(Map<String, String> resultMap, NavigableMap<byte[], NavigableMap<Long, byte[]>> map, String columnFamily){

        for(Map.Entry<byte[], NavigableMap<Long, byte[]>> en : map.entrySet()){
            String qualifier = Bytes.toString(en.getKey());
            NavigableMap<Long, byte[]> ma = en.getValue();
            for(Map.Entry<Long, byte[]>e: ma.entrySet()){
                String version = e.getKey().toString();
                String data = Bytes.toString(e.getValue());
                resultMap.put(columnFamily + "." + qualifier + "." + version, data);
            }
        }
    }

}
