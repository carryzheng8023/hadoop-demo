package xin.carryzheng.hadoop.mr.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zhengxin
 * @date 2019-04-30 17:08:54
 */
public class TableBean implements Writable {

    private String orderId;//订单id
    private String pid;//产品id
    private int amount;//产品数量
    private String pName;//产品名称
    private String flag;//标记是订单表（0）还是产品表（1）

    public TableBean() {
    }

    public TableBean(String orderId, String pid, int amount, String pName, String flag) {
        this.orderId = orderId;
        this.pid = pid;
        this.amount = amount;
        this.pName = pName;
        this.flag = flag;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pName);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.pid = in.readUTF();
        this.amount = in.readInt();
        this.pName = in.readUTF();
        this.flag = in.readUTF();

    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {

        return orderId + '\t' + pName + '\t' + amount + '\t';
    }
}
