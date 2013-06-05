package com.renren.tailor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

import com.renren.tailor.exec.MainServer;
import com.renren.tailor.exec.TaskPatcher;
import com.renren.tailor.hive.ConnectionManager;
import com.renren.tailor.util.PropertiesUtil;

public class TailorBootstrap {
	
	private static Log logger = LogFactory.getLog(TailorBootstrap.class) ;

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		PropertyConfigurator.configure(Thread.currentThread().getContextClassLoader().getResource("log4j_tailor.properties"));
		PropertiesUtil.loadProp("conf/hive_server.properties");
		ConnectionManager.initialize(PropertiesUtil.conf);
		
		MainServer.getMainServer().initialize();
		MainServer.getMainServer().start();
		TaskPatcher.start();
		logger.error(MainServer.getMainServer());
		Thread.sleep(180000l);
	}

}
