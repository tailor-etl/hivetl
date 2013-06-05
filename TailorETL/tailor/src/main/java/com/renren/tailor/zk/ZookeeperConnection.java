package com.renren.tailor.zk;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class ZookeeperConnection {
  
  private ZooKeeper conn;
  private Watcher watcher=null;
  private String strConn=null;
  private int sessionTimeOut;

  private static Logger log = Logger.getLogger(ZookeeperConnection.class);
  
  public ZookeeperConnection(String strConn,int sessionTimeOut,Watcher watcher) {
	  this.strConn=strConn;
	  this.sessionTimeOut=sessionTimeOut;
	  if(watcher==null){
		  this.watcher=new ReconnectionWarcher();
	  }else{
		  this.watcher=watcher;
	  }
	  
  }

  private synchronized void connect() throws IOException {
	  conn = new ZooKeeper(strConn, sessionTimeOut, this.watcher);
	  log.info("Success to get new zookeeper!");
  }

  public ZooKeeper get() throws IOException {
	  if (conn == null) {
		  connect();
	  }
	  return conn;
  }
  
  public void reconnect() throws IOException {
   try {
      conn.getChildren("/", null);
      log.info("the old connection is ok,so do not reconnect");
      return;
    } catch (Exception e1) {
    }  
	  close();
	  connect();
  }

  public void close() {
	  try {
		  conn.close();
		  conn=null;
	  } catch (InterruptedException e) {
		  e.printStackTrace();
	  }
  }

  class ReconnectionWarcher implements Watcher{
	  public void process(WatchedEvent event) {
	    log.info("ReconnectionWarcher accept zk Event:" + event.getState() + " " + event.getType());
		  if(event.getState() == KeeperState.Expired){
			  log.info("Start to reconnect zookeeper!");
			  for(int i=1;i<6;i++){
				  try {
					  reconnect();
					  break;
				  } catch (IOException e) {
					  log.error("Fail to reconnect zookeeper!try "+i+"times",e);
				  }
				  try {
					  Thread.sleep(1000);
				  } catch (InterruptedException e) {
					  e.printStackTrace();
				  }
			  }
		   }
	  }
	  
  	}
}

