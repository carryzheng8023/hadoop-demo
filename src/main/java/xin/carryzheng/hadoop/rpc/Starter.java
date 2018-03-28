package xin.carryzheng.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;

/**
 * @author zhengxin
 * @date 2018-03-27 14:31:15
 */
public class Starter {

    public static void main(String[] args) throws IOException {

        Builder builder = new RPC.Builder(new Configuration());

        builder.setBindAddress("127.0.0.1")
                .setPort(10000)
                .setProtocol(LoginService.class)
                .setInstance(new LoginServiceImpl());


        Server server = builder.build();

        server.start();

    }

}
