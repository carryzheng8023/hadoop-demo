package xin.carryzheng.hadoop.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * 接口回调数据生产者
 *
 * @author zhengxin
 * @date 2019-04-23 15:34:23
 */
public class MsgProducer2 {

    public static void main(String[] args) {

        //1.配置生产者属性
        Properties props = new Properties();
        //(1)配置kafka集群节点的地址，可以是多个
        props.put("bootstrap.servers", "hadoop01:9092");
        //(2)配置发送的消息是否等待应答
        props.put("acks", "all");
        //(3)配置消息发送失败的重试
        props.put("retries", 0);
        //(4)配置批量处理数据的大小
        props.put("batch.size", 16384);
        //(5)配置请求延时，单位：毫秒
        props.put("linger.ms", 5);
        //(6)配置发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        //(7)配置key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //(8)配置value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //(9)配置自定义分区
//		props.put("partitioner.class", "xin.carryzheng.hadoop.kafka.CustomPartitioner");

        //2.实例化KafkaProducer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //3.调用Producer的send方法，进行消息的发送，接口回调
        producer.send(new ProducerRecord<>("testTopic", "hello kafka!"), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {

                if(null != metadata){
                    System.out.println(metadata.offset() + "---" + metadata.partition());
                }

            }
        });



        //4.close释放资源
        producer.close();

    }

}
