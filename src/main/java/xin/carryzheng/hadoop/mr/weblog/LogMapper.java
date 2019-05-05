package xin.carryzheng.hadoop.mr.weblog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhengxin
 * @date 2019-05-05 11:41:05
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();


        //3.解析log
        boolean result = parseLog(line, context);

        if(!result)
            return;

        k.set(line);

        //4.输出
        context.write(k, NullWritable.get());

    }

    private boolean parseLog(String line, Context context) {

        //2.切割
        String[] fields = line.split(" ");

        if(fields.length > 11){
            context.getCounter("log_map","parseLog_true").increment(1);
            return true;
        }else{
            context.getCounter("log_map","parseLog_false").increment(1);
            return false;
        }



    }
}
