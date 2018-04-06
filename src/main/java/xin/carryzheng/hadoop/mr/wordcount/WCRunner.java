package xin.carryzheng.hadoop.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author zhengxin
 * @date 2018-03-30 10:07:16
 */
public class WCRunner {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://master01:9000");
//        conf.set("dfs.permissions", "false");
//        conf.set("mapreduce.job.jar", "wc.jar");
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("yarn.resourcemanager.hostname", "master01");
//        conf.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");

        Job job = Job.getInstance(conf);

        //设置整个job所用的那些类在哪个jar包
        job.setJarByClass(WCReducer.class);

        //指定map和reduce类
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        //指定reduce输出kv数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //指定map输出的kv数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //指定要处理的输入数据存放的路径
        FileInputFormat.setInputPaths(job, new Path("hdfs://master01:9000/wc/srcdata/"));
//        FileInputFormat.setInputPaths(job, new Path("/Users/zhengxin/Desktop/wc/srcdata/"));


        //指定处理结果的输出数据存放的路径
        FileOutputFormat.setOutputPath(job,new Path("hdfs://master01:9000/wc/output10/"));
//        FileOutputFormat.setOutputPath(job,new Path("/Users/zhengxin/Desktop/wc/output/"));

        //将job提交给集群运行
        job.waitForCompletion(true);



    }

}
