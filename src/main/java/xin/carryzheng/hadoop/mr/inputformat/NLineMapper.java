package xin.carryzheng.hadoop.mr.inputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhengxin
 * @date 2019-05-06 13:28:48
 */
public class NLineMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    final Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        k.set(value);
        context.write(k, NullWritable.get());
    }
}
