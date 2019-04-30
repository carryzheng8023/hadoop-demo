package xin.carryzheng.hadoop.mr.order;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhengxin
 * @date 2019-04-30 10:12:02
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {


    //0000001	222.8
    //0000001	25.8

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        context.write(key, NullWritable.get());

    }
}
