package xin.carryzheng.hadoop.mr.ii;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.zookeeper.txn.Txn;

import java.io.IOException;
import java.net.URI;

/**
 * @author zhengxin
 * @date 2018-04-03 14:23:50
 */
public class InverseIndexStepTwo {

    public static class StepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] fields = StringUtils.split(line, "\t");

            String[] wordAndFilename = StringUtils.split(fields[0], "-->");

            String word = wordAndFilename[0];
            String filename = wordAndFilename[1];

            long count = Long.parseLong(fields[1]);

            context.write(new Text(word), new Text(filename + "-->" + count));
        }
    }


    public static class StepTwoReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String result = "";

            for (Text value : values){
                result += value + " ";
            }

            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master01:9000/");

        Job job_two = Job.getInstance(conf);

        job_two.setJarByClass(InverseIndexStepTwo.class);
        job_two.setMapperClass(StepTwoMapper.class);
        job_two.setReducerClass(StepTwoReducer.class);

        job_two.setOutputKeyClass(Text.class);
        job_two.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job_two, new Path(args[0]));

        Path output = new Path(args[1]);

        FileSystem fs = FileSystem.get(new URI("hdfs://master01:9000/"), conf, "zhengxin");

        if (fs.exists(output)){
            fs.delete(output, true);
        }

        FileOutputFormat.setOutputPath(job_two, output);


        System.exit(job_two.waitForCompletion(true) ? 0 : 1);



    }
}
