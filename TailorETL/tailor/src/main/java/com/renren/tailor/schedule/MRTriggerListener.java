package com.renren.tailor.schedule;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.listeners.TriggerListenerSupport;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.renren.tailor.hsql.DataPersisManager;
import com.renren.tailor.model.JobInfo;
import com.renren.tailor.model.RuleEngine;
import com.renren.tailor.util.HDFSUtil;
import com.renren.tailor.util.JaskSonUtil;
import com.renren.tailor.util.ParameterUtil;
import com.renren.tailor.util.TimeUtil;

public class MRTriggerListener extends TriggerListenerSupport {
	
	private static final Log logger = LogFactory.getLog(MRTriggerListener.class);

	private static final Map<String, String> RUNNING_JOB = new ConcurrentHashMap<String, String>();

	private String name;

	public MRTriggerListener(String name) {
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public boolean vetoJobExecution(Trigger arg0, JobExecutionContext context) {
		JobDataMap map = context.getJobDetail().getJobDataMap();
		String[] command = (String[]) map.get(ParameterUtil.JOB_DATA_COMMAND);
		String rul = "-" + ParameterUtil.RULE;
		map.put(ParameterUtil.RULE, command[2].substring(command[2]
				.indexOf(rul) + rul.length() + 1));
		RuleEngine rule = JaskSonUtil.readValueAsObjFromStr(
				map.getString(ParameterUtil.RULE), RuleEngine.class);
		Map<String, String> patMap = rule.getPartitions();
		
		Iterator<Entry<String, String>> it = patMap.entrySet().iterator();
		String out = "";
		Map<String, String> result = new LinkedHashMap<String, String>();
		while (it.hasNext()) {
			Entry<String, String> en = it.next();
			result.put(en.getKey(),
					TimeUtil.getLastInfo(en.getValue(), patMap.size()));
		out += en.getKey() + "="+ TimeUtil.getLastInfo(en.getValue(), patMap.size()) + "/";
		}
		out = out.substring(0, out.length() - 1);
		
		if(rule.getFieldRule()==null || rule.getFieldRule().size()==0){//直接添加partition
			rule.setOutputPath(TimeUtil.getInputPath(rule.getOutputPath(),new LinkedList<String>(patMap.values())));
		}else{
			String outPutPath=rule.getOutputPath() + out;
			rule.setOutputPath(outPutPath);
		}
		rule.setPartitions(result);
		rule.setInputPath(TimeUtil.getInputPath(rule.getInputPath(),new LinkedList<String>(patMap.values())));
		try {
			command[2]=command[2].substring(0,command[2].indexOf(rul)+rul.length())+" "+JaskSonUtil.getObjectMapper().writeValueAsString(rule);
		} catch (JsonProcessingException e2) {
			e2.printStackTrace();
		}
		map.put(ParameterUtil.JOB_DATA_COMMAND, command);
		
		logger.info("vetoJobException...inputPath:"+rule.getInputPath()+";outPutPath:"+rule.getOutputPath()+";tablename:"+rule.getTableName());
		
		map.put(ParameterUtil.RULE,rule);
		
		try {
			if(DataPersisManager.checkJobRunned(rule.getTableName(), JaskSonUtil.getObjectMapper().writeValueAsString(result), 0)){
				logger.info("has runned");
				return true;
			}
		} catch (JsonProcessingException e) {
			logger.info(e.getMessage());
		}
		
		JobInfo info = new JobInfo();
		info.setStartTime(TimeUtil.formatFromUtcTime(new Date().getTime() + "",null));
		info.setName(rule.getName());
		info.setPartitions(result);
		info.setTableName(rule.getTableName());
		map.put(ParameterUtil.JOB_INFO, info);
		
		
		String[] inPath=rule.getInputPath().split(",");
		FileSystem fs = HDFSUtil.getFileSystem();
		for(String p:inPath){
			try {
				if (!rule.isLocal() && !fs.exists(new Path(p)) && (rule.getFieldRule()!=null && rule.getFieldRule().size()>0) ){
					map.put(ParameterUtil.PATH_ERROR, ParameterUtil.ErrorCode.INPUT_PATH_NOTEXIST);
					logger.info("file not exist");
					return true;
				}
			} catch (IOException e) {
				map.put(ParameterUtil.PATH_ERROR, ParameterUtil.ErrorCode.INPUT_PATH_NOTEXIST);
				logger.info("file not exist"+e.getMessage()+" error!");
				return true;
			}
		}
		try {
			if((rule.getFieldRule()!=null && rule.getFieldRule().size()>0) && fs.exists(new Path(rule.getOutputPath()))){
				map.put(ParameterUtil.PATH_ERROR, ParameterUtil.ErrorCode.OUTPUT_PATH_EXIST);
				logger.info("output file  exist");
				return true;
			}
		} catch (IOException e) {
			map.put(ParameterUtil.PATH_ERROR, ParameterUtil.ErrorCode.OUTPUT_PATH_EXIST);
			logger.info("out file not exist"+e.getMessage()+" error!");
			return true;
		}
		
		if (RUNNING_JOB.get(rule.getTableName() + rule.getOutputPath()) != null){
			logger.info("正在运行...");
			return true;
		}
			
		
		RUNNING_JOB.put(rule.getTableName() + rule.getOutputPath(), rule.getTableName());
		return false;
	}
	
	@Override
	public void triggerComplete(Trigger trigger, JobExecutionContext context,
			CompletedExecutionInstruction triggerInstructionCode) {
		JobDataMap map = context.getJobDetail().getJobDataMap();
		RuleEngine rule =(RuleEngine) map.get(ParameterUtil.RULE);
		RUNNING_JOB.remove(rule.getTableName()+rule.getOutputPath());
		logger.info("remove key:"+rule.getTableName()+rule.getOutputPath());
		
		try {
			if(context.getScheduler().getSchedulerName().equals(DirectSchedulerFactory.DEFAULT_SCHEDULER_NAME)){
				context.getScheduler().shutdown();
			}
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
	}


}
