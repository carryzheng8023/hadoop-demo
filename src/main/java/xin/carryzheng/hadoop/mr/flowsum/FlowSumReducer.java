package xin.carryzheng.hadoop.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhengxin
 * @date 2018-03-30 16:15:10
 */
public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long up_flow_counter = 0;
        long down_flow_counter = 0;

        for (FlowBean bean : values){
            up_flow_counter += bean.getUp_flow();
            down_flow_counter += bean.getD_flow();
        }

        context.write(key, new FlowBean(key.toString(), up_flow_counter, down_flow_counter));


    }
}
