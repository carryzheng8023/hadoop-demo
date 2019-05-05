package xin.carryzheng.hadoop.mr.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

/**
 * 编解码测试
 *
 * @author zhengxin
 * @date 2019-05-05 17:36:47
 */
public class CompressTest {

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        //1.测试压缩
//        compress("/Users/zhengxin/Desktop/test/big.txt", "org.apache.hadoop.io.compress.BZip2Codec");
//        compress("/Users/zhengxin/Desktop/test/big.txt", "org.apache.hadoop.io.compress.GzipCodec");
//        compress("/Users/zhengxin/Desktop/test/big.txt", "org.apache.hadoop.io.compress.DefaultCodec");
        //2.解压测试
//        decompress("/Users/zhengxin/Desktop/test/big.txt.bz2");
//        decompress("/Users/zhengxin/Desktop/test/big.txt.gz");
//        decompress("/Users/zhengxin/Desktop/test/big.txt.deflate");
    }

    private static void decompress(String filename) throws IOException {

        //0.校验
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());

        CompressionCodec codec = factory.getCodec(new Path(filename));

        if(codec == null){
            System.out.println("不支持改解码器" + filename);
            return;
        }
        //1.获取输入流
        CompressionInputStream cis =  codec.createInputStream(new FileInputStream(new File(filename)));

        //2.获取输出流
        FileOutputStream fos = new FileOutputStream(new File(filename + ".decode"));

        //3.流的对拷
        IOUtils.copyBytes(cis, fos, 1024*1024*5, false);

        //4.关闭流
        cis.close();
        fos.close();


    }

    private static void compress(String filename, String method) throws IOException, ClassNotFoundException {

        //1.获取输入流
        FileInputStream fis = new FileInputStream(new File(filename));

        Class classname = Class.forName(method);

        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(classname, new Configuration());

        //2.获取输出流
        FileOutputStream fos = new FileOutputStream(new File(filename + codec.getDefaultExtension()));

        CompressionOutputStream cos = codec.createOutputStream(fos);

        //3.流的对拷
        IOUtils.copyBytes(fis, cos, 1024*1024*5, false);

        //4.关闭资源
        fis.close();
        cos.close();
        fos.close();

    }

}
