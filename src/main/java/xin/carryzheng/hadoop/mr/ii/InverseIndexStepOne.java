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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * @author zhengxin
 * @date 2018-04-03 13:59:35
 */
public class InverseIndexStepOne {

    public static class StepOneMapper extends Mapper<LongWritable, Text, Text, LongWritable>{


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] fields = StringUtils.split(line, " ");

            String filename = ((FileSplit)context.getInputSplit()).getPath().getName();

            for (String field : fields){

                context.write(new Text(field + "-->" + filename), new LongWritable(1));

            }

        }
    }

    public static class StepOneReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long counter = 0;

            for (LongWritable value : values){
                counter += value.get();
            }

            context.write(key, new LongWritable(counter));

        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master01:9000/");

        Job job = Job.getInstance(conf);

        job.setJarByClass(InverseIndexStepOne.class);
        job.setMapperClass(StepOneMapper.class);
        job.setReducerClass(StepOneReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        Path output = new Path(args[1]);

        FileSystem fs = FileSystem.get(new URI("hdfs://master01:9000/"), conf, "zhengxin");

        if (fs.exists(output)){
            fs.delete(output, true);
        }

        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
