package xin.carryzheng.hadoop.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.DoubleWritable;

/**
 * @author zhengxin
 * @date 2019-08-05 17:22:17
 */
public class SumUDAF extends AbstractGenericUDAFResolver {
    /**
     * 获取处理逻辑类
     * @param info
     * @return
     * @throws SemanticException
     */
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        //判断输入参数是否合法,参数个数，参数类型
        if (info.length != 1) {
            throw new UDFArgumentLengthException("输入参数个数非法，一个参数");
        }

        return new GenericEvaluate();
    }


    //处理逻辑类
    public static class GenericEvaluate extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector input;
        private DoubleWritable result ;                   //保存最终结果
        private MyAggregationBuffer myAggregationBuffer;  //自定义聚合列，保存临时结果

        //自定义AggregationBuffer
        public static class MyAggregationBuffer implements AggregationBuffer {
            Double sum;
        }

        @Override  //指定返回类型
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            result = new DoubleWritable(0);
            input = (PrimitiveObjectInspector) parameters[0];
            // 指定返回结果类型
            return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
        }

        @Override   //获得一个聚合的缓冲对象，每个map执行一次
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MyAggregationBuffer myAggregationBuffer = new MyAggregationBuffer();
            reset(myAggregationBuffer);  // 重置聚合值
            return myAggregationBuffer;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            MyAggregationBuffer newAgg = (MyAggregationBuffer) agg;
            newAgg.sum = 0.0;
        }

        @Override  // 传入参数值聚合
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            MyAggregationBuffer myAgg = (MyAggregationBuffer) agg;
            double inputNum = PrimitiveObjectInspectorUtils.getDouble(parameters[0], input);
            myAgg.sum += inputNum;
        }

        @Override  // iterate 输出中间结果
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MyAggregationBuffer newAgg = (MyAggregationBuffer) agg;
            result.set(newAgg.sum);
            return result;
        }

        @Override  // 合并
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            double inputNum = PrimitiveObjectInspectorUtils.getDouble(partial, input);
            MyAggregationBuffer newAgg = (MyAggregationBuffer) agg;
            newAgg.sum += inputNum;
        }

        @Override  //输出最终结果
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MyAggregationBuffer aggregationBuffer = (MyAggregationBuffer) agg;
            result.set(aggregationBuffer.sum);
            return result;
        }
    }
}
