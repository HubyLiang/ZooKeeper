package com.liang.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 软负载均衡 : 轮询算法
 * 原理: 把来自用户的请求轮流分配给内部的服务器：从服务器1开始，直到服务器N，然后重新开始循环。
 * 模拟: 银行中5个窗口,用户依次去 window 1,window 2,...window 5, window 1,window 2 , ... 轮询办理业务.
 *       若其中一个窗口关闭,则依次顺移到下一个 window, 若该 window 又重新打开,则继续开启服务.
 */
public class BalanceTest {

    private static Logger logger = Logger.getLogger(WatchOne.class);

    //实例常量
    private static final String CONNECT_STRING = "127.0.0.1:2181";
    private static final String PATH = "/bank";
    private static final String SUB_PRIFIX = "sub";
    private static final int SESSION_TIMEOUT = 50 * 1000;

    //实例变量
    private ZooKeeper zooKeeper;
    private List<String> list = new ArrayList<String>();
    private int currentService = 0;
    private int windowsCount = 5; //共计5个窗口

    public ZooKeeper startZK() throws IOException {
        return new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                try {
                    list = zooKeeper.getChildren(PATH, true); // sub1, sub2, sub3... sub5
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }


    public String processRequest() throws KeeperException, InterruptedException {

        currentService += 1; //默认从1号窗口开始

        for (int i = currentService; i <= windowsCount; i++) {
            //判断 list中目前是否有 sub+i 这个窗口,如果 down机,则跳过该服务器
            if (list.contains(SUB_PRIFIX + currentService)) { //list.contains(sub1)...
                return new String(zooKeeper.getData(PATH + "/" + SUB_PRIFIX + currentService, false, new Stat()));
            } else {
                currentService += 1;
            }
        }

        //currentService > 6 时
        for (int i = 1; i <= windowsCount; i++) {
            //再次做了判断 list中目前是否有 sub+i 这个窗口,如果 down机,则跳过该服务器
            if (list.contains(SUB_PRIFIX + i)) {
                //重置到i号窗口
                currentService = i;
                return new String(zooKeeper.getData(PATH + "/" + SUB_PRIFIX + currentService, false, new Stat()));
            }
        }

        return "****** ERROR : no this data *******";
    }


    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        BalanceTest test = new BalanceTest();

        test.setZooKeeper(test.startZK());
        Thread.sleep(2000);

        for (int i = 1; i <= 15; i++) {
            String result = test.processRequest();
            logger.info("****customerID:" + i + "\t window:" + test.currentService + "\t result:" + result);
            Thread.sleep(2000);
        }
    }


    //---------------setter/getter---------------
    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }
}
