package xin.carryzheng.hadoop.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * @author zhengxin
 * @date 2018-04-10 15:58:54
 */
public class RandomWordSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;


    private String[] brands = {"iphone", "xiaomi", "huawei", "sony", "samsung", "moto", "meizu"};


    @Override
    public void nextTuple() {

        Random random = new Random();
        int index = random.nextInt(brands.length);

        String goodName = brands[index];

        collector.emit(new Values(goodName));

        Utils.sleep(500);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("originName"));

    }
}
