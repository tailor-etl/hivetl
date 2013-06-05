package com.renren.tailor.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.renren.tailor.exec.MainServer;



public class NeutralWatcher implements Watcher  {
  
  private static Logger log = LoggerFactory.getLogger(NeutralWatcher.class);
  
  private ZookeeperManager cm = null;
  public NeutralWatcher(ZookeeperManager cm){
    this.cm=cm;
  }

  @Override
  public void process(WatchedEvent event) {
    // master 节点超时处理
    // 1.检查是否存在master
    // 2.不存在则 初始化master
    // 3.存在且初始化失败 则 转为slave
    if (event.getState() == KeeperState.Expired) {
      boolean exsit = false;
      boolean reInitialize = false;
      while (!exsit) {
        try {
          cm.reconnect();
          exsit = cm.isMasterExist();
        } catch (Exception e) {
        }
        if (exsit) {
          break;
        }
        try {
          cm.initializeMaster();
          reInitialize = true;
        } catch (Exception e) {
          log.error("Engine master KeeperState.Expired reInitialize fail。 This Exception:" + e.getMessage());
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e1) {
          }
        }
        log.info("Engine master KeeperState.Expired execute succeess");
      }
      if (!reInitialize) {
        while (true) {
          log.info("Engine master KeeperState.Expired reInitialize fail,maybe it has exsit another master，so will stop self service");
          try {
            cm.initializeSlave();
            MainServer.getMainServer().standby();
          } catch (Exception e) {
            log.info("Engine master KeeperState.Expired reInitialize fail,maybe it has exsit another master，but stop service fail and sleep 5s :" + e.getMessage());
            try {
              Thread.sleep(5000);
            } catch (InterruptedException e1) {
            }
          }
          log.info("Engine master KeeperState.Expired reInitialize fail,maybe it has exsit another master，stop this service succeess");
          break;
        }

      }
    }
  }

}
