package com.renren.tailor.exec;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

import com.renren.tailor.schedule.QuartzManager;
import com.renren.tailor.zk.ZookeeperManager;

/**
* @ClassName: MainServer
* @Description: 
* @author xianbing.liu@renren-inc.com
* @date 2013-6-3 下午02:46:07
*
*/ 
public class MainServer {

  private static ReentrantLock lock = new ReentrantLock();

  private static MainServer ea;

  private static  ZookeeperManager cm = null;
  
  private boolean haMaster = false;

  public void start() {
	  if(haMaster){
		  QuartzManager.startQuartz();
	      try {
			Main.startAll();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	  }
  }

  public void standby() {
    QuartzManager.shutdownJobs();
    try {
		Main.shutDownThreads();
		TaskPatcher.shutDownThreads();
	} catch (InterruptedException e) {
		e.printStackTrace();
	}
  }

  public static MainServer getMainServer() {
    if (ea == null) {
      lock.lock();
      if (ea == null) {
        ea = new MainServer();
        cm=new ZookeeperManager();
      }
      lock.unlock();
    }
    return ea;
  }
  
  public void initialize()throws Exception{
	  cm.initialize();
	  if (cm.isMasterExist()) {
          this.initializeSlave();
      } else {
          this.initializeMaster();
      }
  }

  public void initializeMaster() throws Exception {
    try {
      cm.initializeMaster();
      haMaster=true;
    } catch (Exception e) {
      throw new Exception("Fail to initialize Master  ", e);
    }
  }

  public void initializeSlave() throws Exception {
    try {
      cm.initializeSlave();
      haMaster=false;
    } catch (Exception e) {
      throw new Exception("Fail to initialize Slave  ", e);
    }
  }
}
