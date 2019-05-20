package xin.carryzheng.hadoop.hbase.mr.hdfs2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 从hdfs写入hbase
 *
 * @author zhengxin
 * @date 2019-05-17 10:15:11
 */
public class FruitRunner implements Tool {

    private Configuration conf;


    @Override
    public int run(String[] args) throws Exception {

        //创建job
        Job job = Job.getInstance(conf);
        job.setJarByClass(FruitRunner.class);

        //mapper
        job.setMapperClass(FruitMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //reducer
        TableMapReduceUtil.initTableReducerJob("fruit", FruitReducer.class, job);

        //InputFormat
        FileInputFormat.addInputPath(job, new Path("hdfs://hadoop01:9000/input_fruit"));

        //OutputFormat

        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = HBaseConfiguration.create(conf);
        this.conf.set("hbase.rootdir", "hdfs://hadoop01:9000/hbase");
        this.conf.set("hbase.zookeeper.quorum", "hadoop01:2181");
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new FruitRunner(), args);
        System.exit(status);
    }
}
