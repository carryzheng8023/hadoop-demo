package xin.carryzheng.hadoop.mr.wordcount;

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

import java.io.IOException;
import java.net.URI;

/**
 * @author zhengxin
 * @date 2018-04-06 16:01:24
 */
public class WordCountMR {

    private static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] words = StringUtils.split(line, " ");

            for (String word : words){
                context.write(new Text(word), new LongWritable(1));
            }
        }

    }

    private static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long count = 0;

            for (LongWritable value : values)
                count += value.get();

            context.write(key, new LongWritable(count));

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://server00:9000/");
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCountMR.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path output = new Path(args[1]);
        FileSystem fs = FileSystem.get(new URI("hdfs://server00:9000/"), conf, "zhengxin");
        if (fs.exists(output)) fs.delete(output, true);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
