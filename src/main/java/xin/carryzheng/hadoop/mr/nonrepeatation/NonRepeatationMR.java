package xin.carryzheng.hadoop.mr.nonrepeatation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * @author zhengxin
 * @date 2018-04-22 14:01:00
 */
public class NonRepeatationMR {

    public static class NonRepeatationMapper extends Mapper<LongWritable, Text, Text, NullWritable>{


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            context.write(value, NullWritable.get());

        }

    }

    public static class NonRepeatationReducer extends Reducer<Text, NullWritable, Text, NullWritable>{


        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://server00:9000/");

        /**以下提交集群使用*/
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "server00");
        conf.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");
        conf.set("mapreduce.job.jar", "/Users/zhengxin/Documents/GitProject/hadoopdemo/target/hadoop-demo-1.0-SNAPSHOT.jar");
        /**以上提交集群使用*/

        Job job = Job.getInstance(conf);
        job.setJarByClass(NonRepeatationMR.class);
        job.setMapperClass(NonRepeatationMapper.class);
        job.setReducerClass(NonRepeatationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path output = new Path(args[1]);
        FileSystem fs = FileSystem.get(new URI("hdfs://server00:9000/"), conf, "zhengxin");
        if (fs.exists(output)) fs.delete(output, true);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
