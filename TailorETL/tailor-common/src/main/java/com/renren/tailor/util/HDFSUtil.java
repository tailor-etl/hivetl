package com.renren.tailor.util;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUtil {

  /**
   * hdfs get or put 
   * @chunguo.wang@renren-inc.com
   */
  
  private static final Log log4j = LogFactory.getLog(HDFSUtil.class); 
  
  private static final Configuration conf = new Configuration();
  
  static{
	   // conf.setStrings("fs.default.name", "hdfs://BJCER256-230.opi.com");
	    conf.setStrings(FileSystem.FS_DEFAULT_NAME_KEY,"hdfs://BJCER256-230.opi.com");
	    conf.addResource("/core-site.xml");
	    conf.addResource("/hdfs-site.xml");
	    conf.addResource("/mapred-site.xml");
  }
  
//  public synchronized static FileSystem getFileSystem(String ip, String port) {  
//      FileSystem fs = null;  
//      String url = "hdfs://" + ip + ":" + port;  
//      Configuration conf = new Configuration();  
//      conf.set("fs.default.name", url);  
//      try {  
//          fs = FileSystem.get(conf);  
//      } catch (Exception e) {  
//        log4j.error("can not get hdfs FileSystem ");  
//      }  
//      return fs;  
//  }  
  
  
  public static FileSystem getFileSystem(){
	  FileSystem fs=null;
	try {
		fs = FileSystem.get(conf);
	} catch (IOException e) {
		e.printStackTrace();
	}
	  return fs;
  }
  
  /** 
   * put File to hdfs
   * @param fs 
   * @param local 
   * @param remote 
 * @throws IOException 
   */  
  public synchronized static void putLocalFileToHdfs(String local,  
          String remote) throws IOException {  
	  FileSystem fs=FileSystem.get(conf);
      // Path home = fs.getHomeDirectory();  
      Path workDir = fs.getWorkingDirectory();  
      String fin=workDir+"/"+remote;
      Path dst = new Path( fin);  
      Path src = new Path(local);  
      try {  
          fs.copyFromLocalFile(false, true, src, dst);  
          log4j.info("put " + local + " to  " + fin + " successed.. ");  
      } catch (Exception e) {  
    	  e.printStackTrace();
        log4j.error("put " + local + " to  " + fin + " failed :." );  
      }  finally{
    	  fs.close();
      }
  }  
  
  
  public static void main(String[] args) {

  }

}
