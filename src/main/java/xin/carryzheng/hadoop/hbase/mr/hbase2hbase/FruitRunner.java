package xin.carryzheng.hadoop.hbase.mr.hbase2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 将hbase中数据处理后写回hbase
 *
 * @author zhengxin
 * @date 2019-05-16 15:46:47
 */
public class FruitRunner implements Tool {

    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(conf);

        job.setJarByClass(FruitRunner.class);

        Scan scan = new Scan();

        //设置mapper
        TableMapReduceUtil.initTableMapperJob("fruit", scan, ReadFruitMapper.class, ImmutableBytesWritable.class, Put.class, job);

        //设置reducer
        TableMapReduceUtil.initTableReducerJob("fruit_mr", WriteFruitReducer.class, job);

        job.setNumReduceTasks(1);

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
