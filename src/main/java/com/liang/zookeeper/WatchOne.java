package com.liang.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * 客户端注册监听它关心的目录节点，当目录节点发生变化（数据改变、被删除、子目录节点增加删除）时，zookeeper会通知客户端。
 */
public class WatchOne {

    private static Logger logger = Logger.getLogger(WatchOne.class);

    //实例常量
    private static final String CONNECT_STRING = "127.0.0.1:2181";
    private static final String PATH = "/Java_ZooKeeper";
    private static final int SESSION_TIMEOUT = 50 * 1000;

    //实例变量
    private ZooKeeper zooKeeper;

    public ZooKeeper startZK() throws IOException {
        return new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    //创建ZooKeeper节点
    public void createZNOde(String nodePath, String nodeValue) throws KeeperException, InterruptedException {
        zooKeeper.create(nodePath, nodeValue.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * getZNode方法后,在/Java_ZooKeeper这个ZNode节点上设置一个watcher,只要当前节点变化后通知Client
     * 异步回调的触发机制: 当数据有了变化时zkserver向客户端发送一个watch,它是一次性的动作，即触发一次就不再有效,
     * 如果想继续Watch的话，需要客户端重新设置Watcher
     * @param nodePath
     * @return
     */
    public String getZNode(final String nodePath) throws KeeperException, InterruptedException {
        String retValue = null;
        byte[] bytes = zooKeeper.getData(nodePath, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                try {
                    triggerValue(nodePath);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, new Stat());

        retValue = new String(bytes);
        return retValue;
    }


    public void triggerValue(String nodePath) throws KeeperException, InterruptedException {
        String retValue = null;
        byte[] bytes = zooKeeper.getData(nodePath, false, new Stat());
        retValue = new String(bytes);

        logger.debug("********************triggerValue:"+retValue);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        WatchOne watchOne = new WatchOne();
        ZooKeeper zooKeeper = watchOne.startZK();
        watchOne.setZooKeeper(zooKeeper);

        if (watchOne.getZooKeeper().exists(PATH,false)==null){
            watchOne.createZNOde(PATH,"Watcher");
            logger.debug("*************main result:"+watchOne.getZNode(PATH));
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    //---------------setter/getter---------------
    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

}
