package xin.carryzheng.hadoop.storm;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * @author zhengxin
 * @date 2018-04-10 16:24:59
 */
public class SuffixBolt extends BaseBasicBolt {

    private FileWriter fileWriter = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {

        try {
            fileWriter = new FileWriter("/home/zhengxin/storm_output/" + UUID.randomUUID());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        String upper_name = tuple.getString(0);
        String suffix_name = upper_name + "_" + System.currentTimeMillis();

        try {
            fileWriter.write(suffix_name);
            fileWriter.write("\n");
            fileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {


    }
}
