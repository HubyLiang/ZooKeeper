package com.liang.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;

/**
 * 监控某一个父节点下面的子节点变化(增删改)
 */
public class WatchChildrenNode {

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
                //子节点变化 并且指定特定的父节点下的子节点
                if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged && watchedEvent.getPath().equalsIgnoreCase(PATH)){
                    showChildrenNode(PATH);
                }else {
                    showChildrenNode(PATH); //首次进入必须注册父节点,如果要监控/Java_ZooKeeper下的子节点,则必须注册/Java_ZooKeeper节点
                }
            }
        });
    }

    public void showChildrenNode(String nodePath) {
        List<String> children = null;
        try {
            children = zooKeeper.getChildren(nodePath, true);
            logger.debug("***********showChildrenNode:" + children);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        WatchChildrenNode watchChildrenNode = new WatchChildrenNode();
        watchChildrenNode.setZooKeeper(watchChildrenNode.startZK());

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
