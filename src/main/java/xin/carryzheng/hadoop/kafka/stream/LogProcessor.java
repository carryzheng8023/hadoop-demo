package xin.carryzheng.hadoop.kafka.stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @author zhengxin
 * @date 2019-04-23 16:33:10
 */
public class LogProcessor implements Processor<byte[], byte[]> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(byte[] key, byte[] value) {

        String input = new String(value);
        //如果包含>>>，则去除
        if(input.contains(">>>"))
            input = input.split(">>>")[1];
        this.context.forward(key, input.getBytes());
    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }
}
