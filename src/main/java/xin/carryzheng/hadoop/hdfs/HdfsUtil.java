package xin.carryzheng.hadoop.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author zhengxin
 * @date 2018-03-27 09:14:23
 */
public class HdfsUtil {

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://server01:9000/");

        FileSystem fs = FileSystem.get(conf);

        Path src = new Path("hdfs://server01:9000/AI-2017-11-24_01.mp4");

        FSDataInputStream in = fs.open(src);

        FileOutputStream os = new FileOutputStream("/Users/zhengxin/Desktop/AI.mp4");

        IOUtils.copy(in, os);
    }


}
