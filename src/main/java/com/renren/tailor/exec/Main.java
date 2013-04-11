package com.renren.tailor.exec;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

import com.renren.tailor.model.RuleEngine;
import com.renren.tailor.schedule.QuartzManager;
import com.renren.tailor.util.JaskSonUtil;
import com.renren.tailor.util.ParameterUtil;

public class Main {

	private static Log logger = LogFactory.getLog(Main.class) ;
	
	private static final Map<String, Long> FILE_INFO = new HashMap<String, Long>();

	private static final ScheduledExecutorService schedule   = Executors.newScheduledThreadPool(1);

	private static  String APPLICATION_HOME ;
	
	
	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException {
		PropertyConfigurator.configure(Thread.currentThread().getContextClassLoader().getResource("log4j_tailor.properties"));
		APPLICATION_HOME = System.getProperty("user.dir");
		File f =null;
		String conf = APPLICATION_HOME + "/conf/";
		RuleEngine ru = null;
		if(args==null || args.length==0){
			 f = new File(conf);
			if(null!=f && null!=f.listFiles()){
				File[] list = f.listFiles();
				for (File fs : list) {
					ru = JaskSonUtil.getObjectMapper().readValue(fs, RuleEngine.class);
					StringBuilder sb=new StringBuilder();
					if(!checkExtJar(ru,sb)){
						continue;
					}
					String[] ar = new String[3];
					ar[0] = "sh";
					ar[1] = APPLICATION_HOME + "/bin/had.sh";
					ar[2]="";
					if(sb.length()>0){
						ar[2]=" -libjars "+sb.toString();
					}
					ar[2]+=" -"+ParameterUtil.RULE+" "+JaskSonUtil.getObjectMapper().writeValueAsString(ru);
					Map<String, Object> maps = new HashMap<String, Object>();
					maps.put(ParameterUtil.JOB_DATA_COMMAND, ar);
					FILE_INFO.put(ru.getTableName(), fs.lastModified());
					QuartzManager.scheduleCronJob(QuartzJobRunner.class,  ru.getTableName(),maps, ru.getCron(), null);
				}
			}
			schedule.scheduleAtFixedRate( new RuleFilePool(), 0, 200, TimeUnit.SECONDS);
		}else{
			 f = new File(conf+args[0]);
			 ru = JaskSonUtil.getObjectMapper().readValue(f, RuleEngine.class);
				StringBuilder sb=new StringBuilder();
				if(!checkExtJar(ru,sb)){
					return;
				}
			if(args.length==2){
				List<LinkedHashMap<String, String>> partions = JaskSonUtil.getObjectMapper().readValue(args[1], List.class);
				 for(LinkedHashMap<String, String> map:partions){
					    ru.setPartitions(map);
					    String[] ar = new String[3];
						ar[0] = "sh";
						ar[1] = APPLICATION_HOME + "/bin/had.sh";
						ar[2]="";
						if(sb.length()>0){
							ar[2]=" -libjars "+sb.toString();
						}
						ar[2]+=" -"+ParameterUtil.RULE+" "+JaskSonUtil.getObjectMapper().writeValueAsString(ru);
						Map<String, Object> maps = new HashMap<String, Object>();
						maps.put(ParameterUtil.JOB_DATA_COMMAND, ar);
						QuartzManager.scheduleOnceJob(QuartzJobRunner.class,ru.getTableName()+map.values().toString(),maps);
				 }
			}else if(args.length==3){
				LinkedHashMap<String, String> map=(LinkedHashMap<String, String>) JaskSonUtil.getObjectMapper().readValue(args[1], Map.class);
				   ru.setPartitions(map);
				   ru.setInputPath(args[2]);
				    String[] ar = new String[3];
					ar[0] = "sh";
					ar[1] = APPLICATION_HOME + "/bin/had.sh";
					ar[2]="";
					if(sb.length()>0){
						ar[2]=" -libjars "+sb.toString();
					}
					ar[2]+=" -"+ParameterUtil.RULE+" "+JaskSonUtil.getObjectMapper().writeValueAsString(ru);
					Map<String, Object> maps = new HashMap<String, Object>();
					maps.put(ParameterUtil.JOB_DATA_COMMAND, ar);
					QuartzManager.scheduleOnceJob(QuartzJobRunner.class,ru.getTableName()+map.values().toString(),maps);
			}
		}
		
	}

	
	private static class RuleFilePool extends Thread {
		public void run() {
			try {
				String conf = APPLICATION_HOME + "/conf/";
				File f = new File(conf);
				File[] list = f.listFiles();
		
				RuleEngine ru = null;
				Set<String> newFiles = new HashSet<String>();
				Set<String> files = new HashSet<String>(FILE_INFO.keySet());
				if(list!=null){
					for (File fs : list) {
						ru = JaskSonUtil.getObjectMapper().readValue(fs,
								RuleEngine.class);
						newFiles.add(ru.getTableName());
					}
				}
				files.removeAll(newFiles);
				
				for(String tname:files){
					FILE_INFO.remove(tname);
					QuartzManager.deleteJob(tname);
				}
				if(list!=null){
					for (File fs : list) {
						String[] ar = new String[3];
						ar[0] = "sh";
						ar[1] = APPLICATION_HOME + "/bin/had.sh";
						ar[2] = "";
						ru = JaskSonUtil.getObjectMapper().readValue(fs,
								RuleEngine.class);
						Map<String, Object> maps = new HashMap<String, Object>();
						maps.put(ParameterUtil.JOB_DATA_COMMAND, ar);
						StringBuilder sb=new StringBuilder();
						if (FILE_INFO.get(ru.getTableName()) != null
								&& FILE_INFO.get(ru.getTableName()) != fs.lastModified()) {// 规则文件修改
							QuartzManager.deleteJob(ru.getTableName());
							FILE_INFO.put(ru.getTableName(), fs.lastModified());
							if(!checkExtJar(ru,sb)){
								continue;
							}
							if(sb.length()>0){
								ar[2]=" -libjars "+sb.toString();
							}
							ar[2]+=" -"+ParameterUtil.RULE+" "+JaskSonUtil.getObjectMapper().writeValueAsString(ru);
							QuartzManager.scheduleCronJob(QuartzJobRunner.class,  ru.getTableName(),maps, ru.getCron(), null);
						} else if (FILE_INFO.get(ru.getTableName()) == null) {// 新添加规则文件
							FILE_INFO.put(ru.getTableName(), fs.lastModified());
							if(!checkExtJar(ru,sb)){
								continue;
							}
							if(sb.length()>0){
								ar[2]=" -libjars "+sb.toString();
							}
							ar[2]+=" -"+ParameterUtil.RULE+" "+JaskSonUtil.getObjectMapper().writeValueAsString(ru);
							QuartzManager.scheduleCronJob(QuartzJobRunner.class,  ru.getTableName(),maps, ru.getCron(), null);
						}
					}
				}
			} catch (Throwable e) {
				e.printStackTrace();
			}

		}
	}
	
	private static boolean checkExtJar(RuleEngine ru,StringBuilder sb ){
		if(StringUtils.isNotBlank(ru.getExtJarName())){
			String[] extJar=ru.getExtJarName().split(",");
			String filePath;
			for(String s:extJar){
				filePath=APPLICATION_HOME + "/lib/ext/"+s+".jar";
				File extFile=new File(filePath);
				if(!extFile.exists()){
					logger.error("file "+filePath+" not exist");
					return false;
				}
				sb.append(filePath+",");
			}
			if(sb.length()>0){
				sb.deleteCharAt(sb.length()-1);
			}
		}
		return true;
	}
}
