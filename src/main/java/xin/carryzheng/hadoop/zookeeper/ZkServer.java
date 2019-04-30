package xin.carryzheng.hadoop.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * 模拟服务器上下线（服务端）
 *
 * @author zhengxin
 * @date 2019-04-24 15:24:08
 */
public class ZkServer {

    private static final String connectString = "hadoop01:2181";
    private static final int sessionTimeout = 2000;
    private static ZooKeeper zkClient;

    private static final String parentNode = "/servers";


    static {
        try {
            zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
                @Override
                public void process(WatchedEvent event) {

                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws KeeperException, InterruptedException {

        ZkServer server = new ZkServer();

        //1.获取连接zkServer


        //2.注册服务器节点信息
        server.register(args[0]);

        //3.业务逻辑处理
        server.business(args[0]);
    }


    /**
     * 注册服务器节点
     *
     * @author zhengxin
     * @date   2019/4/24 15:30
     */
    private void register(String hostname) throws KeeperException, InterruptedException {
        String path = zkClient.create(parentNode + "/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(path);
    }

    private void business(String hostname) throws InterruptedException {
        System.out.println(hostname + " is online!");
        Thread.sleep(Long.MAX_VALUE);
    }


}
