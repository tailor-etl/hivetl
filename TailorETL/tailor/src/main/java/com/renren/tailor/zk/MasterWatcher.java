package com.renren.tailor.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.renren.tailor.exec.MainServer;

class MasterWatcher implements Watcher {

  private static Logger log = LoggerFactory.getLogger(MasterWatcher.class);
  private ZookeeperManager cm = null;

  public MasterWatcher(ZookeeperManager cm) {
    this.cm = cm;
  }

  @Override
  public void process(WatchedEvent event) {
    log.info("accept zk Event:" + event.getState() + " " + event.getType());
    // slave 超时处理
    // 1 重连zk
    // 2.重新加master监听
    // 3.重新创建节点地址
    if (event.getState() == KeeperState.Expired) {
      while (true) {
        try {
          cm.reconnect();
          cm.initializeSlave();
          log.info("KeeperState.Expired reInitialize succeess");
          break;
        } catch (Exception e) {
          log.error("KeeperState.Expired reInitialize fail and sleep 30s。this exception:" + e.getMessage());
          try {
            Thread.sleep(30000);
          } catch (InterruptedException e1) {
          }
        }
      }
    }

    // master NodeDeleted
    // 1.slave 转 master
    // 2.转失败 则 还原为master
    if (event.getType() == EventType.NodeDeleted) {
      boolean initMaster = false;
      boolean initStart = false;
      try {
        MainServer.getMainServer().initializeMaster();
        initMaster = true;
      } catch (Exception e) {
        log.error("Fail to automatic init tailor Master ,maybe has created new  Master ", e);
        return;
      }
      try {
        MainServer.getMainServer().start();
        initStart = true;
        cm.deleteEngineSlaveAddress();
        log.info("Success to automatic switching ha master!");
        return;
      } catch (Exception e) {
        log.error("Fail to restart tailor app!" + e.getMessage());
      }
      
      // 初始化master成功，但是启动服务失败，则删除master 转为slave
      if (initMaster && !initStart) {
          log.info("initMaster Success but initStart fail，so will stop service。");
          try {
              cm.deleteMasterAddress();
              MainServer.getMainServer().initializeSlave();
          } catch (Exception e) {
              log.error("Fail to delete master、stop service、init Slave " + e.getMessage());
          }
          // slave 节点必须 standby
          while (true) {
              try {
                MainServer.getMainServer().standby();
                  break;
              } catch (Exception e) {
                  log.error("Fail to standby service and sleep 10s " + e.getMessage());
                  try {
                      Thread.sleep(10000);
                  } catch (InterruptedException e1) {
                  }
              }
          }

      }
    }

  }

}