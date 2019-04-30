package xin.carryzheng.hadoop.mr.outputformat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhengxin
 * @date 2019-04-30 16:30:53
 */
public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    private static String NEW_LINE = System.getProperty("line.separator");

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        String k = key.toString();

        k += NEW_LINE;

        context.write(new Text(k), NullWritable.get());
    }
}
