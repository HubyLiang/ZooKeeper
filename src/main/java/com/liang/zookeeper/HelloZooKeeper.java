package com.liang.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class HelloZooKeeper {
    private static final Logger logger = Logger.getLogger(HelloZooKeeper.class);

    //实例常量
    private static final String CONNECT_STRING = "127.0.0.1:2181";
    private static final String PATH = "/Java_ZooKeeper";
    private static final int SESSION_TIMEOUT = 50 * 1000;

    public ZooKeeper startZK() throws IOException {
        return new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
            }
        });
    }

    public void stopZK(ZooKeeper zooKeeper) throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }

    //创建一个持久化的目录节点
    public void createZNode(ZooKeeper zooKeeper, String nodePath, String nodeValue) throws KeeperException, InterruptedException {
        zooKeeper.create(nodePath, nodeValue.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    //获取到指定path下的目录节点的value
    public String getZNode(ZooKeeper zooKeeper, String nodePath) throws KeeperException, InterruptedException {
        String retValue = null;
        byte[] bytes = zooKeeper.getData(nodePath, false, new Stat());
        retValue = new String(bytes);

        return retValue;
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        HelloZooKeeper helloZooKeeper = new HelloZooKeeper();

        ZooKeeper zooKeeper = helloZooKeeper.startZK();

        if (zooKeeper.exists(PATH, false) == null) {
            helloZooKeeper.createZNode(zooKeeper, PATH, "zookeeper01");

            String result = helloZooKeeper.getZNode(zooKeeper, PATH);

            logger.debug("********* helloZooKeeper result: " + result);
        } else {
            logger.debug("********* this node is already exist");
        }

        helloZooKeeper.stopZK(zooKeeper);
    }
}
