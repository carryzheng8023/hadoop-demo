package xin.carryzheng.hadoop.mr.areapartition;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhengxin
 * @date 2018-04-02 19:25:53
 */
public class AreaPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE> {

    private static Map<String, Integer> areaMap = new HashMap<>();


    static {
        areaMap.put("135", 0);
        areaMap.put("136", 1);
        areaMap.put("137", 2);
        areaMap.put("138", 3);
        areaMap.put("139", 4);
    }

    @Override
    public int getPartition(KEY key, VALUE value, int i) {

        Integer areaCode = areaMap.get(key.toString().substring(0, 3));

        return (areaCode == null ? 5 : areaCode);

    }
}
