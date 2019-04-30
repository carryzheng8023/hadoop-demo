package xin.carryzheng.hadoop.mr.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhengxin
 * @date 2019-04-30 17:34:01
 */
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //1001	01	5
        //01	小米

        //准备集合
        List<TableBean> orderBeanList = new ArrayList<>();
        TableBean pdBean = new TableBean();

        for (TableBean value: values){
            if("0".equals(value.getFlag())){
                //订单表
                TableBean tableBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tableBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

                orderBeanList.add(tableBean);
            }else {
                //产品表
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }

        }

        //拼接表
        for (TableBean tableBean : orderBeanList){
            tableBean.setpName(pdBean.getpName());
            context.write(tableBean, NullWritable.get());
        }

    }
}
