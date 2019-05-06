package xin.carryzheng.hadoop.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;

import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * NLineInputFormat示例
 * 根据每个输入文件的行数来规定输出多少个切片。例如每三行放入一个切片中。
 *
 * @author zhengxin
 * @date 2019-05-06 13:17:45
 */
public class NLineDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf);

        job.setJarByClass(NLineDriver.class);

        job.setMapperClass(NLineMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        // 设置每个切片 InputSplit 中划分三条记录
        NLineInputFormat.setNumLinesPerSplit(job, 3);
        //设置输入格式
        job.setInputFormatClass(NLineInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/zhengxin/Desktop/test/input_kv"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/zhengxin/Desktop/test/output"));

        job.waitForCompletion(true);

    }
}
