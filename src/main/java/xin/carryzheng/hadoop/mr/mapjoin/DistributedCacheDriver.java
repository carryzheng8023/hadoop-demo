package xin.carryzheng.hadoop.mr.mapjoin;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Map端表合并(Distributedcache)
 *
 * 适用于关联表中有小表的情形;
 * 可以将小表分发到所有的 map 节点，这样，map 节点就可以在本地对自己所读到的大
 * 表数据进行合并并输出最终结果，可以大大提高合并操作的并发度，加快处理速度。
 *
 * @author zhengxin
 * @date 2019-04-30 20:46:18
 */
public class DistributedCacheDriver {

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        //1 获取job信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置加载 jar 包路径
        job.setJarByClass(DistributedCacheDriver.class);

        //3 关联map
        job.setMapperClass(DistributedCacheMapper.class);

        // 4 设置最终输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 5 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("file:///Users/zhengxin/Desktop/test/input_order2"));
        FileOutputFormat.setOutputPath(job, new Path("file:///Users/zhengxin/Desktop/test/output"));

        // 6 加载缓存数据
        job.addCacheFile(new URI("file:///Users/zhengxin/Desktop/test/pd.txt"));

        // 7 map端join的逻辑不需要reduce阶段，设置reducetask数量为0
        job.setNumReduceTasks(0);

        //8 提交
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
