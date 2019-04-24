package xin.carryzheng.hadoop.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhengxin
 * @date 2019-04-24 15:38:46
 */
public class ZkClient {

    private static final String connectString = "hadoop01:2181";
    private static final int sessionTimeout = 2000;
    private static ZooKeeper zkClient;


    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {

        ZkClient client = new ZkClient();
        //1.获取连接
        client.connect();

        //2.监听服务节点路径
        client.getServerList();

        //3.业务处理
        client.business();
    }

    private void connect() throws IOException {

        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

                try {
                    getServerList();
                }catch (Exception e){
                    e.printStackTrace();
                }

            }
        });
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 获取服务器列表
     * @author zhengxin
     * @date   2019/4/24 15:42
     */
    private void getServerList() throws KeeperException, InterruptedException {

        List<String> children = zkClient.getChildren("/servers", true);

        //存储服务器列表
        List<String> serverList = new ArrayList<>();

        //获取每个节点中的数据
        for(String child : children){
            byte[] data = zkClient.getData("/servers/" + child, false, null);
            serverList.add(new String(data));
        }

        System.out.println(serverList);
    }

}
