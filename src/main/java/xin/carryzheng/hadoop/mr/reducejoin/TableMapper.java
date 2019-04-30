package xin.carryzheng.hadoop.mr.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author zhengxin
 * @date 2019-04-30 17:15:52
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    private TableBean v = new TableBean();
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //区分两张表
        FileSplit split = (FileSplit)context.getInputSplit();
        String name = split.getPath().getName();

        String line = value.toString();
        String[] fields = line.split("\t");

        if(name.startsWith("order")){
            //订单表
            //1001	01	5
            //1002	02	4
            v.setOrderId(fields[0]);
            v.setPid(fields[1]);
            v.setAmount(Integer.parseInt(fields[2]));
            v.setpName("");
            v.setFlag("0");

            k.set(fields[1]);

        }else {
            //产品表
            //01	小米
            //02	华为
            v.setOrderId("");
            v.setPid(fields[0]);
            v.setAmount(0);
            v.setpName(fields[1]);
            v.setFlag("1");

            k.set(fields[0]);
        }

        //输出
        context.write(k, v);
    }
}
