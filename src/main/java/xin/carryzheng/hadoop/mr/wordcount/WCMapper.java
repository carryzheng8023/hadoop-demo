package xin.carryzheng.hadoop.mr.wordcount;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhengxin
 * @date 2018-03-30 09:43:50
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {



    /**
     * mr框架每读一行数据就调用一次该方法
     * @author zhengxin
     * @date   18/3/30 上午9:53
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] words = StringUtils.split(line, " ");

        for (String word : words){
            context.write(new Text(word), new LongWritable(1));
        }
    }
}
