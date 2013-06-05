package com.renren.tailor.exec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestMain {

	private static Log logger = LogFactory.getLog(TestMain.class) ;
	
  /**
   * @Title: main
   * @Description: TODO
   * @param @param args    
   * @return void    
   * @throws
   */
  public static void main(String[] args) {

	  logger.error("come into my home!");
  }

  
  public static void shutDownThreads(){
	  logger.error("close my home!");
  }
}
