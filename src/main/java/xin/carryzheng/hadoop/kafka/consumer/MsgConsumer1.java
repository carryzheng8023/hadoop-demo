package xin.carryzheng.hadoop.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author zhengxin
 * @date 2019-04-23 15:44:21
 */
public class MsgConsumer1 {

    public static void main(String[] args) {




        //1.配置消费者属性
        Properties props = new Properties();
        //(1)配置kafka服务器地址
        props.put("bootstrap.servers", "hadoop01:9092");
        //(2)设置消费组
        props.put("group.id", "g1");
        //(3)是否自动确认offset
        props.put("enable.auto.commit", "true");
        //(4)自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        //(5)key的反序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //(6)value反的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //2.创建消费者实例
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //4.释放资源
        Runtime.getRuntime().addShutdownHook(new Thread(()->
                consumer.close()
        ));

        consumer.subscribe(Arrays.asList("testTopic"));

        //3.拉消息
        for (;;) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());

        }



    }
}
