package com.liang.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * 对同一个ZNode节点进行多次监测,如果每次目录节点发生变化（数据改变、被删除、子目录节点增加删除）时，zookeeper都会通知客户端。
 */
public class WatchMore {
    private static Logger logger = Logger.getLogger(WatchOne.class);

    //实例常量
    private static final String CONNECT_STRING = "127.0.0.1:2181";
    private static final String PATH = "/Java_ZooKeeper";
    private static final int SESSION_TIMEOUT = 50 * 1000;

    //实例变量
    private ZooKeeper zooKeeper;
    private String oldValue;

    //创建ZooKeeper节点
    public void createZNOde(String nodePath, String nodeValue) throws KeeperException, InterruptedException {
        zooKeeper.create(nodePath, nodeValue.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public ZooKeeper startZK() throws IOException {
        return new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    public String getZNode(final String nodePath) throws KeeperException, InterruptedException {
        String retValue = null;

        //Zookeeper里的所有读取操作：getData(),getChildren()和exists()都有设置watch的选项
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
        oldValue = retValue;
        return retValue;
    }

    /**
     * 递归调用 triggerValue(final String nodePath) 都会创建一个新的watcher
     * @param nodePath
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void triggerValue(final String nodePath) throws KeeperException, InterruptedException {
        String retValue = null;
        byte[] bytes = zooKeeper.getData(nodePath, new Watcher() {
            public void process(WatchedEvent event) {
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
        String newValue = retValue;
        if (oldValue.equalsIgnoreCase(newValue)){
            logger.debug("*******************no changes************");
        }else{
            logger.debug("*************oldValue:"+oldValue+"\t newValue:"+newValue);
            //更新oldValue
            oldValue = newValue;
        }
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        WatchMore watchMore = new WatchMore();
        ZooKeeper zooKeeper = watchMore.startZK();
        watchMore.setZooKeeper(zooKeeper);

        if (watchMore.getZooKeeper().exists(PATH, false) == null) {
            watchMore.createZNOde(PATH, "Watcher");
            logger.debug("*************main result:" + watchMore.getZNode(PATH));
        }else{
            logger.debug("*************main result: 该节点已经存在" );
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

    public String getOldValue() {
        return oldValue;
    }

    public void setOldValue(String oldValue) {
        this.oldValue = oldValue;
    }
}
