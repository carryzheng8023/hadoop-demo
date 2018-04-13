package xin.carryzheng.hadoop.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import java.util.Map;

import static org.apache.storm.Config.TOPOLOGY_NAME;

/**
 *
 * conf.setNumWorkers(4) 表示设置了4个worker来执行整个topology的所有组件
 *
 * builder.setBolt("boltA",new BoltA(),  4)  ---->指明 boltA组件的线程数excutors总共有4个
 * builder.setBolt("boltB",new BoltB(),  4) ---->指明 boltB组件的线程数excutors总共有4个
 * builder.setSpout("randomSpout",new RandomSpout(),  2) ---->指明randomSpout组件的线程数excutors总共有4个
 *
 * -----意味着整个topology中执行所有组件的总线程数为4+4+2=10个
 * ----worker数量是4个，有可能会出现这样的负载情况，  worker-1有2个线程，worker-2有2个线程，worker-3有3个线程，worker-4有3个线程
 *
 * 如果指定某个组件的具体task并发实例数
 * builder.setSpout("randomspout", new RandomWordSpout(), 4).setNumTasks(8);
 * ----意味着对于这个组件的执行线程excutor来说，一个excutor将执行8/4=2个task
 *
 * @author zhengxin
 * @date 2018-04-10 16:34:13
 */
public class TopologyMain {

    public static void main(String[] args) throws Exception{

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("randomspout", new RandomWordSpout(), 4);
        builder.setBolt("upperbolt", new UpperBolt(), 4).shuffleGrouping("randomspout");
        builder.setBolt("suffixbolt", new SuffixBolt(), 4).shuffleGrouping("upperbolt");

        StormTopology topology = builder.createTopology();

        Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setDebug(true);
        conf.setNumAckers(0);


        StormSubmitter.submitTopology("demotopo", conf, topology);


    }

}
