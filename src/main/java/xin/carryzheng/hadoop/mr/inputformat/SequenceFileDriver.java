package xin.carryzheng.hadoop.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 *
 * 处理小文件，将小文件合成一个大文件输出到磁盘存储
 *
 * @author zhengxin
 * @date 2019-04-30 14:43:41
 */
public class SequenceFileDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(SequenceFileDriver.class);
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        //设置InputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);
        //输出文件为sequenceFile类型，key：路径，value：文件内容
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        FileInputFormat.setInputPaths(job, new Path("file:///Users/zhengxin/Desktop/test/input_inputformat"));
        FileOutputFormat.setOutputPath(job, new Path("file:///Users/zhengxin/Desktop/test/output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
