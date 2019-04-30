package xin.carryzheng.hadoop.mr.order;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhengxin
 * @date 2019-04-30 10:08:09
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {


    private OrderBean k = new OrderBean();
//    private DoubleWritable v = new DoubleWritable();

    //0000001	Pdt_01	222.8
    //0000001	Pdt_06	25.8
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");

        k.setOrderId(Integer.parseInt(fields[0]));
        k.setPrice(Double.parseDouble(fields[2]));

//        v.set(Double.parseDouble(fields[2]));

        context.write(k, NullWritable.get());

    }
}
