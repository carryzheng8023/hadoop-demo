package xin.carryzheng.hadoop.mr.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * 辅助排序和二次排序
 *
 * 利用“订单 d和成交金额”作为key，可以将map阶段读取到的所有订单数据按照id分区，按照金额排序，发送到reduce。
 * 在reduce端利用GroupingComparator将订单 id 相同的 kv 聚合成组，然后取第一个即是最大值
 *
 * @author zhengxin
 * @date 2019-04-30 10:18:47
 */
public class OrderDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(OrderDriver.class);

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);


        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //设置分区
        job.setPartitionerClass(OrderPartitioner.class);
        job.setNumReduceTasks(3);

        //设置reducer端分组
        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        FileInputFormat.setInputPaths(job, new Path("file:///Users/zhengxin/Desktop/test/input_group"));
        FileOutputFormat.setOutputPath(job, new Path("file:///Users/zhengxin/Desktop/test/output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
