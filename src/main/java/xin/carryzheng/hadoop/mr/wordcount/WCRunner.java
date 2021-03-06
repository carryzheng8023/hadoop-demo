package xin.carryzheng.hadoop.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 *
 * 单词计数
 *
 * 处理小文件，用CombineTextInputFormat在逻辑上成为一个大文件
 * job.setInputFormatClass(CombineTextInputFormat.class);
 * CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);// 4m
 * CombineTextInputFormat.setMinInputSplitSize(job, 2097152);// 2m
 *
 * @author zhengxin
 * @date 2018-03-30 10:07:16
 */
public class WCRunner {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("yarn.resourcemanager.hostname", "hadoop01");
//        conf.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");
//        conf.set("mapreduce.job.jar", "/Users/zhengxin/Documents/GitProject/hadoopdemo/target/hadoop-demo-1.0-SNAPSHOT.jar");


        // 开启map端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);
        // 设置map端输出压缩方式
//        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
        conf.setClass("mapreduce.map.output.compress.codec", DefaultCodec.class, CompressionCodec.class);


        Job job = Job.getInstance(conf);

        //设置整个job所用的那些类在哪个jar包
        job.setJarByClass(WCReducer.class);

        //指定map和reduce类
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

        //指定map输出的kv数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //指定reduce输出kv数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        // 如果不设置 InputFormat,它默认用的是 TextInputFormat.class
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);// 4m
//        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);// 2m

//        job.setNumReduceTasks(3);

        //指定要处理的输入数据存放的路径
//        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop01:9000/wc/input/"));
        FileInputFormat.setInputPaths(job, new Path("file:///Users/zhengxin/Desktop/test/input/"));


        //指定处理结果的输出数据存放的路径
//        FileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop01:9000/wc/output/"));
        FileOutputFormat.setOutputPath(job,new Path("file:///Users/zhengxin/Desktop/test/output/"));


        // 设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);
        // 设置压缩的方式
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
//        FileOutputFormat.setOutputCompressorClass(job, GzipCodedec.class);
//        FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);


        //将job提交给集群运行
        job.waitForCompletion(true);



    }

}
