package com.liang.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * 分布式通知和协调作用
 */
public class ClientA {
    private static Logger logger = Logger.getLogger(WatchOne.class);

    //实例常量
    private static final String CONNECT_STRING = "127.0.0.1:2181";
    private static final String PATH = "/distribute";
    private static final int SESSION_TIMEOUT = 50 * 1000;

    //实例变量
    private ZooKeeper zooKeeper;
    private String oldValue;


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
     *
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
        if ("AAA".equalsIgnoreCase(newValue)) {
            logger.debug("******************* AAA ************");
        }
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ClientA clientA = new ClientA();
        clientA.setZooKeeper(clientA.startZK());

        logger.info("**************" + clientA.getZNode(PATH));


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
