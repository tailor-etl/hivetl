package com.renren.tailor.zk;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;

import com.renren.tailor.exec.TestMain;
import com.renren.tailor.util.PropertiesUtil;

public class ZookeeperManager {
	
	private static Log logger = LogFactory.getLog(ZookeeperManager.class) ;

  private static  ZookeeperConnection zc = null;

  private String ZK_MASTER_NODE = "/tailor/master";
  private String ZK_SLAVE_NODE = "/tailor/slave";

  private static String address;

  static {
    try {
      InetAddress addr = InetAddress.getLocalHost();
      address = addr.getHostAddress();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
  }

  public void initialize() throws KeeperException, InterruptedException, IOException {
    zc = new ZookeeperConnection(PropertiesUtil.conf.get("zookeeper"), 6000, null);
    logger.error("zc:"+zc);
    if (zc.get().exists("/tailor", false) == null) {
      zc.get().create("/tailor", "tailor".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
  }
  
  public static void main(String[] args) throws KeeperException, InterruptedException, IOException{
	  new ZookeeperManager().initialize();
	  System.out.println((zc.get().exists("/tailor", false)));
  }

  public void createMasterNode() throws KeeperException, InterruptedException, IOException {
    zc.get().create(ZK_MASTER_NODE, address.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
  }

  public void addWatcher(Watcher watcher) throws KeeperException, InterruptedException, IOException {
    zc.get().exists(ZK_MASTER_NODE, watcher);
  }

  public void deleteMasterNode() throws KeeperException, InterruptedException, IOException {
    zc.get().delete(ZK_MASTER_NODE, -1);
  }

  public void deleteExecutorSlaveNode() throws KeeperException, InterruptedException, IOException {
    if (zc.get().exists(ZK_SLAVE_NODE + "/" + address.toString(), false) == null) {
      return;
    }
    zc.get().delete(ZK_SLAVE_NODE + "/" + address.toString(), -1);
  }

  public void createSlaveNode() throws KeeperException, InterruptedException, IOException {
    if (zc.get().exists(ZK_SLAVE_NODE, false) == null) {
      zc.get().create(ZK_SLAVE_NODE, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    if (zc.get().exists(ZK_SLAVE_NODE + "/" + address.toString(), false) == null) {
      zc.get().create(ZK_SLAVE_NODE + "/" + address.toString(), null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }
  }

  public void deleteEngineSlaveAddress() throws KeeperException, InterruptedException, IOException {
    if (zc.get().exists(ZK_SLAVE_NODE, false) == null) {
      return;
    }
    zc.get().delete(ZK_SLAVE_NODE, -1);
  }

  public void deleteMasterAddress() throws KeeperException, InterruptedException, IOException {
    if (zc.get().exists(ZK_MASTER_NODE, false) == null) {
        return;
    }
    zc.get().delete(ZK_MASTER_NODE, -1);
}

  
  public boolean exsit() throws KeeperException, InterruptedException, IOException {
    if (zc.get().exists(ZK_MASTER_NODE, false) == null)
      return false;
    return true;
  }

  public void initializeMaster() throws KeeperException, InterruptedException, IOException {
    this.createMasterNode();
    this.addWatcher(new NeutralWatcher(this));
  }

  public void initializeSlave() throws KeeperException, InterruptedException, IOException {
    this.createSlaveNode();
    this.addWatcher(new MasterWatcher(this));
  }

  public boolean isMasterExist() throws KeeperException, InterruptedException, IOException {
    if (zc.get().exists(ZK_MASTER_NODE, false) == null)
      return false;
    return true;
  }

  public void reconnect() throws IOException {
    zc.reconnect();
  }
}
