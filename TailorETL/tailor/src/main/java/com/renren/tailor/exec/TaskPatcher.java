package com.renren.tailor.exec;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.renren.tailor.hive.HiveMetaStore;
import com.renren.tailor.hsql.DataPersisManager;
import com.renren.tailor.model.RuleEngine;
import com.renren.tailor.util.JaskSonUtil;
import com.renren.tailor.util.TailorUtil;

public class TaskPatcher{
	
	private static Log logger = LogFactory.getLog(TaskPatcher.class) ;

	private static final int JOB_RUNTIME=3;
	
	public static  ScheduledExecutorService schedule   = Executors.newScheduledThreadPool(1);
	
	public static void start(){
		logger.info("come into start");
		schedule.scheduleAtFixedRate( new InnerThread(), 0, 600, TimeUnit.SECONDS);
	}
	
	 //关闭线程
	  public static void shutDownThreads() throws InterruptedException{
	    if(schedule!=null){
	      schedule.shutdown();
	      if(!schedule.awaitTermination(30, TimeUnit.SECONDS)){
	        schedule.shutdownNow();
	      }
	    }
	  }
	  
	static class InnerThread extends Thread{
	@Override
	public void run(){
		Map<String,List<Map<String, String>>> errorJob=new LinkedHashMap<String, List<Map<String,String>>>();
		try {
			List<RuleEngine> list=getContinuteTable();
			logger.info("list size="+list.size());
			for(RuleEngine ru:list){
				List<Map<String, String>> map=TailorUtil.getFailedTask(HiveMetaStore.getPatchPartitions(ru));
				errorJob.put(ru.getTableName(), getDeathJob(map,ru.getTableName()));
				if(map.size()>0){
//					String[] args=new String[2];
//					args[0]=ru.getName();
//					args[1]=JaskSonUtil.getObjectMapper().writeValueAsString(map);
					logger.info("inner thread:"+ru.toString()+map.toString());
					Main.startByName(ru.getName(), JaskSonUtil.getObjectMapper().writeValueAsString(map));
					Thread.sleep(10000);//10s后再执行
				}
			}
			
			if(errorJob.size()>0){
				TailorUtil.postEmail("ETL Clean Log Error", "xianbing.liu", errorJob.toString());
			}
		}catch (Throwable e) {
		}
		}
	}
	
	private static  List<Map<String, String>> getDeathJob(List<Map<String, String>> map,String tableName){
		List<Map<String, String>> res=new LinkedList<Map<String,String>>();
		try{
			for(Map<String, String> m:map){
				if(DataPersisManager.getFailedJobCount(tableName,JaskSonUtil.getObjectMapper().writeValueAsString(m))>JOB_RUNTIME){
					res.add(m);
					map.remove(m);
				}
			}
		}catch (Exception e) {
		}
		return res;
	}
	
	private static  List<RuleEngine> getContinuteTable(){
		 List<RuleEngine> listEngine=new ArrayList<RuleEngine>();
		String	conf = System.getProperty("user.dir")+"/conf";
		logger.info("configuration location:"+conf);
		File f =new File(conf);
		RuleEngine ru = null;
		try {
		if(null!=f && null!=f.listFiles()){
			File[] list = f.listFiles();
			for (File fs : list) {
					ru = JaskSonUtil.getObjectMapper().readValue(fs, RuleEngine.class);
					if(ru.isContinute()){
						listEngine.add(ru);
					}
			}
		}
		}catch (Throwable e) {
			e.printStackTrace();
		}
		return listEngine;
	}
}
