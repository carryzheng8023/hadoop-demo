package xin.carryzheng.hadoop.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


/**
 * @author zhengxin
 * @date 2018-04-15 14:13:14
 */
public class DemoProducer {


    public static void main(String[] args) {

        final Properties props = new Properties();
        props.put("bootstrap.servers", "server00:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i=1; i<=1000; i++)
            producer.send(new ProducerRecord<>("test", Integer.toString(i), Integer.toString(i)));

        producer.close();




    }

}
