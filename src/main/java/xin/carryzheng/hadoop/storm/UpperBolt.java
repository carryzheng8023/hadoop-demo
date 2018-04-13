package xin.carryzheng.hadoop.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * @author zhengxin
 * @date 2018-04-10 16:18:38
 */
public class UpperBolt extends BaseBasicBolt {


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        String goodName = tuple.getString(0);

        String goodName_upper = goodName.toUpperCase();

        basicOutputCollector.emit(new Values(goodName_upper));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("upperName"));

    }
}
