package xin.carryzheng.hadoop.mr.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhengxin
 * @date 2018-03-30 09:44:08
 */
public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long count = 0;

        for (LongWritable value : values)
            count += value.get();

        context.write(key, new LongWritable(count));

    }
}
