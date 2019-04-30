package xin.carryzheng.hadoop.mr.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author zhengxin
 * @date 2019-04-30 16:18:31
 */
public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {


    private Configuration configuration;
    private FSDataOutputStream czfos = null;
    private FSDataOutputStream otherfos = null;

    public FilterRecordWriter(TaskAttemptContext job) throws IOException {
        this.configuration = job.getConfiguration();
        //获取文件系统
        FileSystem fs = FileSystem.get(configuration);

        //创建两个输出流
        czfos = fs.create(new Path("/Users/zhengxin/Desktop/test/cz.log"));
        otherfos = fs.create(new Path("/Users/zhengxin/Desktop/test/other.log"));

    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {

        //判断key中是否包含carryzheng
        if(key.toString().contains("carryzheng")){
            czfos.write(key.getBytes());
        }else {
            otherfos.write(key.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        //关闭资源
        if (null != czfos)
            czfos.close();
        if (null != otherfos)
            otherfos.close();
    }
}
