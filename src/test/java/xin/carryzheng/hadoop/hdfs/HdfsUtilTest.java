package xin.carryzheng.hadoop.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

/**
 * @author zhengxin
 * @date 2018-03-27 10:18:59
 */
public class HdfsUtilTest {

    FileSystem fs = null;

    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://server00:9000/");
        fs = FileSystem.get(new URI("hdfs://server00:9000/"), conf, "zhengxin");

    }

    @Test
    public void testA(){
        System.out.println("ss");
    }


    @Test
    public void testUpload() throws Exception {

        Path dest = new Path("hdfs://server00:9000/aa/qingshu.txt");

        FSDataOutputStream os = fs.create(dest);

        FileInputStream is = new FileInputStream("/Users/zhengxin/Desktop/const.txt");

        IOUtils.copy(is, os);

    }

    @Test
    public void testUpload2() throws Exception {

        fs.copyFromLocalFile(new Path("/Users/zhengxin/Desktop/const.txt"), new Path("hdfs://server00:9000/aa/const.txt"));

    }

    @Test
    public void testDownload() throws IOException {

        fs.copyToLocalFile(new Path("hdfs://server00:9000/aa/const.txt"), new Path("/Users/zhengxin/Desktop/testDownload.txt"));

    }

    @Test
    public void testListFiles() throws IOException {

        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);

        while (files.hasNext()){
            LocatedFileStatus file = files.next();
            Path path = file.getPath();
            String filename =  path.getName();
            System.out.println(filename);
        }

        System.out.println("-------------------------------");

        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus status : listStatus){
            String name = status.getPath().getName();
            System.out.println(name);
        }



    }

    @Test
    public void testMkdir() throws IOException {

        fs.mkdirs(new Path("/aaa/bbb/ccc"));

    }

    @Test
    public void testRm() throws IOException {

        fs.delete(new Path("/aaa"), true);

    }



}
