package xin.carryzheng.hadoop.mr.areapartition;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import xin.carryzheng.hadoop.mr.flowsum.FlowBean;

import java.io.IOException;

/**
 * @author zhengxin
 * @date 2018-04-02 19:19:51
 */
public class FlowSumArea {

    public static class FlowSumAreaMapper extends Mapper<LongWritable, Text, Text, FlowBean>{


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = StringUtils.split(line, "\t");

            String phoneNB = fields[1];
            long u_flow = Long.parseLong(fields[7]);
            long d_flow = Long.parseLong(fields[8]);

            context.write(new Text(phoneNB), new FlowBean(phoneNB, u_flow, d_flow));

        }

    }

    public static class FlowSumAreaReducer extends Reducer<Text, FlowBean, Text, FlowBean>{

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long up_flow_counter = 0;
            long down_flow_counter = 0;

            for (FlowBean bean : values){
                up_flow_counter += bean.getUp_flow();
                down_flow_counter += bean.getD_flow();
            }

            context.write(key, new FlowBean(key.toString(), up_flow_counter, down_flow_counter));

        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowSumArea.class);
        job.setMapperClass(FlowSumAreaMapper.class);
        job.setReducerClass(FlowSumAreaReducer.class);

        job.setPartitionerClass(AreaPartitioner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setNumReduceTasks(6);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }



}
