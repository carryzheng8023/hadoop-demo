package xin.carryzheng.hadoop.mr.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zhengxin
 * @date 2019-04-30 09:57:07
 */
public class OrderBean implements WritableComparable<OrderBean> {


    private int orderId;//订单id
    private double price;//价格

    public OrderBean() {
    }

    public OrderBean(int orderId, double price) {
        this.orderId = orderId;
        this.price = price;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readInt();
        this.price = in.readDouble();
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        //比较id，价格

        int result = orderId > o.getOrderId() ? 1 : -1;
        if(orderId == o.getOrderId())
            result = price > o.getPrice() ? -1 : 1;

        return result;
    }


}
