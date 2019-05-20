package xin.carryzheng.hadoop.hbase.mr.hdfs2hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhengxin
 * @date 2019-05-17 10:14:26
 */
public class FruitMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] splits = value.toString().split("\t");

        byte[] rowKey = Bytes.toBytes(splits[0]);
        byte[] name = Bytes.toBytes(splits[1]);
        byte[] color = Bytes.toBytes(splits[2]);

        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(rowKey);

        Put put = new Put(rowKey);
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), name);
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), color);


        context.write(immutableBytesWritable, put);

    }
}
