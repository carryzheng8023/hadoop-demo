package xin.carryzheng.hadoop.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

/**
 * 读取SequenceFile中的BytesWritable
 *
 * @author zhengxin
 * @date 2019-05-08 10:19:27
 */
public class SequenceFileReadMR {

    public static class SequenceFileMapper extends Mapper<Text,BytesWritable,Text,Text>{

        @Override
        protected void map(Text key, BytesWritable value, Context context)
                throws IOException, InterruptedException {

            context.write(key, new Text(new String(value.getBytes(),0,value.getLength())));
        }
    }

    public static void main(String[] args)  throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(SequenceFileReadMR.class);

        String in = "file:///Users/zhengxin/Desktop/test/output/";
        String out = "file:///Users/zhengxin/Desktop/test/output2/";

        job.setMapperClass(SequenceFileMapper.class);
        SequenceFileInputFormat.addInputPath(job, new Path(in));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        //如果是在不知道key和value的类型,就用SequenceFileAsTextInputFormat,那Map的key和value的
        //类型就可已都指定为Text
//        job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
        job.setNumReduceTasks(0);

        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(out));

        job.waitForCompletion(true);
    }



}
