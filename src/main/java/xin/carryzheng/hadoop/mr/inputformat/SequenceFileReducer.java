package xin.carryzheng.hadoop.mr.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhengxin
 * @date 2019-04-30 14:40:39
 */
public class SequenceFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {

        for (BytesWritable bytesWritable : values) {
            context.write(key, bytesWritable);
        }

    }
}
