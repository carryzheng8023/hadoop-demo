package xin.carryzheng.hadoop.mr.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author zhengxin
 * @date 2019-04-30 10:12:31
 */
public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {

    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        return (orderBean.getOrderId() & Integer.MAX_VALUE) % numPartitions;
    }
}
