package xin.carryzheng.hadoop.mr.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author zhengxin
 * @date 2019-04-30 14:33:47
 */
public class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件的路径和名称
        FileSplit split= (FileSplit) context.getInputSplit();

        Path path = split.getPath();

        k.set(path.toString());
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {

        context.write(k, value);
    }
}
