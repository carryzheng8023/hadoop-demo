package xin.carryzheng.hadoop.kafka.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * @author zhengxin
 * @date 2019-04-23 16:40:02
 */
public class Application {

    public static void main(String[] args) {
        String fromTopic = "testStream";
        String toTopic = "testTopic";

        //设置参数
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "logProcessor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092");

        //实例化StreamConfig
        StreamsConfig config = new StreamsConfig(props);
        //构建拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE", fromTopic)
                .addProcessor("PROCESSOR", new ProcessorSupplier() {
                    @Override
                    public Processor get() {
                        return new LogProcessor();
                    }
                }, "SOURCE")
                .addSink("SINK", toTopic, "PROCESSOR");

        //根据"StreamConfig对象"以及用于构建拓扑的"Builder对象"实例化Kafka Stream
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }

}
