package xin.carryzheng.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author zhengxin
 * @date 2018-03-27 14:39:05
 */
public class LoginController {

    public static void main(String[] args) throws IOException {

        LoginService proxy = RPC.getProxy(LoginService.class, 1L, new InetSocketAddress("127.0.0.1", 10000), new Configuration());

        String result = proxy.login("zhangsan", "1234444");

        System.out.println(result);


    }

}
