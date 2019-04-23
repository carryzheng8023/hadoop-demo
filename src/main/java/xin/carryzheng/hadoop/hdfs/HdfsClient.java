package xin.carryzheng.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;

/**
 * @author zhengxin
 * @date 2019-04-18 10:51:29
 */
public class HdfsClient {


    public static void main(String[] args) throws Exception {

        //1.获取文件系统
        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS", "hdfs://hadoop01:9000/");
//        FileSystem fs = FileSystem.get(configuration);
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000/"), configuration, "zx");


        //2.上传文件
        fs.copyFromLocalFile(new Path("/Users/zhengxin/Desktop/Good Clinical Practice_A Question & Answer Reference Guide_May 2017.pdf"),
                new Path("/gcp3.pdf"));

        //3.关闭资源
        fs.close();
    }

    @Test
    public void initHDFS() throws Exception {

        // 1 创建配置信息对象
        Configuration configuration = new Configuration();

        // 2 获取文件系统
        FileSystem fs = FileSystem.get(configuration);

        // 3 打印文件系统
        System.out.println(fs.toString());
    }

    @Test
    public void testCopyFromLocalFile() throws Exception {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration,
                "zxxx");

        // 2 上传文件
        fs.copyFromLocalFile(new Path("/Users/zhengxin/Desktop/analyzing-Personality-through-Social-Media-Profile-Pocture-Choiceby-UXRen.pdf"),
                new Path("/social.pdf"));

        // 3 关闭资源 fs.close();
        System.out.println("over");
    }

    @Test
    public void testCopyToLocalFile() throws Exception{

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration,
                "zxxx");

        // 2 执行下载操作

        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件效验
        fs.copyToLocalFile(false, new Path("/big.txt"),
                new Path("/Users/zhengxin/Desktop/test/big.txt"), true);

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testMkdirs() throws Exception{

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration,
                "zxxx");

        // 2 创建目录
        fs.mkdirs(new Path("/0906/daxian/banzhang"));

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testDelete() throws Exception {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration,
                "zxxx");

        // 2 执行删除
        fs.delete(new Path("/0906/"), true);

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testRename() throws Exception {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration,
                "zxxx");

        // 2 修改文件名称
        fs.rename(new Path("/gcp.pdf"), new Path("/gcpppp.pdf"));

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testListFiles() throws Exception {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration,
                "hadoop");
        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();

            // 输出详情

            // 文件名称
            System.out.println(status.getPath().getName());

            // 长度
            System.out.println(status.getLen());

            // 权限
            System.out.println(status.getPermission());

            // z 组
            System.out.println(status.getGroup());

            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {

                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("----------------班长的分割线-----------");
        }
    }

    @Test
    public void testListStatus() throws Exception {
        // 1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration,
                "hadoop");

        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {

            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            } else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }

        // 3 关闭资源
        fs.close();
    }



}
