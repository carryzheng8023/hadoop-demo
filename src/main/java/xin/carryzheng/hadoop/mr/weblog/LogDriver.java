package xin.carryzheng.hadoop.mr.weblog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * 日志清洗
 *
 * @author zhengxin
 * @date 2019-05-05 11:48:01
 */
public class LogDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2 加载jar包
        job.setJarByClass(LogDriver.class);

        //3 关联map
        job.setMapperClass(LogMapper.class);

        // 4 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置reducetask个数为 0
        job.setNumReduceTasks(0);

        // 5 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("file:///Users/zhengxin/Desktop/test/input_weblog"));
        FileOutputFormat.setOutputPath(job, new Path("file:///Users/zhengxin/Desktop/test/output"));

        //6 提交
        System.exit(job.waitForCompletion(true)?0:1);

    }

}
