package xin.carryzheng.hadoop.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author zhengxin
 * @date 2019-04-23 16:18:11
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {

    private long successCount = 0;
    private long errorCount = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            if(null == exception)
                successCount++;
            else
                errorCount++;
    }

    @Override
    public void close() {
        System.out.println("成功的个数：" + successCount);
        System.out.println("失败的个数：" + errorCount);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
