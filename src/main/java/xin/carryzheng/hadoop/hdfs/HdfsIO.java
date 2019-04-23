package xin.carryzheng.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * @author zhengxin
 * @date 2019-04-18 11:54:28
 */
public class HdfsIO {

    @Test
    public void putFileToHDFS() throws Exception {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration,
                "zxxx");

        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("/Users/zhengxin/Desktop/mysql-connector-java-5.1.35.jar"));

        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/mysql-connector-java-5.1.35.jar"));

        //4 流对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }


    @Test
    public void getFileFromHDFS() throws Exception {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration,
                "zxxx");

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/big2.txt"));

        // 3 获取输出流
        //4 流对拷
        IOUtils.copyBytes(fis, System.out, configuration);


        // 5 关闭资源
        IOUtils.closeStream(fis);
        fs.close();
    }

    @Test
    public void readFileSeek1() throws Exception {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration,
                "zxxx");

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 3 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("/Users/zhengxin/Desktop/test/hadoop-2.7.2.tar.gz.part1"));

        // 4 流的拷贝
        byte[] buf = new byte[1024];
        for(int i =0 ; i < 1024 * 128; i++){
            fis.read(buf);
            fos.write(buf);
        }

        // 5 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }


    @Test
    public void readFileSeek2() throws Exception {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration,
                "zxxx");

        // 2 打开输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        // 3 定位输入数据位置
        fis.seek(1024*1024*128);

        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("/Users/zhengxin/Desktop/test/hadoop-2.7.2.tar.gz.part2"));

        // 5 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 6 关闭资源
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }


    // 一致性模型
    @Test
    public void writeFile() throws Exception{

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"),
                configuration, "zxxx");

        // 2 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/hello.txt"));

        //3 写数据
        fos.write("hello".getBytes());

        // 4 一致性刷新
        fos.hflush();

        // 5 关闭资源
        fos.close();
        fs.close();
    }
}
