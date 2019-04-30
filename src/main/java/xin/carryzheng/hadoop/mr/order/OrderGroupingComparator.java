package xin.carryzheng.hadoop.mr.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author zhengxin
 * @date 2019-04-30 10:32:27
 */
public class OrderGroupingComparator extends WritableComparator {

    protected OrderGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean)a;
        OrderBean bBean = (OrderBean)b;

        int result;

        //id相同即认为是一个对象
        if(aBean.getOrderId() > bBean.getOrderId())
            result = 1;
        else if(aBean.getOrderId() < bBean.getOrderId())
            result = -1;
        else
            result = 0;

        return result;
    }
}
