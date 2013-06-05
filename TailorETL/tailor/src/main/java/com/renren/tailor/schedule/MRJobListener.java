package com.renren.tailor.schedule;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.listeners.JobListenerSupport;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.renren.tailor.hive.HiveMetaStore;
import com.renren.tailor.hsql.DataPersisManager;
import com.renren.tailor.model.JobInfo;
import com.renren.tailor.model.RuleEngine;
import com.renren.tailor.util.JaskSonUtil;
import com.renren.tailor.util.ParameterUtil;
import com.renren.tailor.util.TailorUtil;
import com.renren.tailor.util.TimeUtil;

public class MRJobListener extends JobListenerSupport {

	private static Log logger = LogFactory.getLog(MRJobListener.class);
	
	private String name;

	public MRJobListener(String name) {
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void jobExecutionVetoed(JobExecutionContext context) {
		JobDataMap map = context.getJobDetail().getJobDataMap();
		RuleEngine rule = (RuleEngine) map.get(ParameterUtil.RULE);
		logger.info("jobExecutionVetoed...tableName:" + rule.getTableName()
				+ ";partitions:" + rule.getPartitions().toString());
		JobInfo info =null;
		if(null!=map.get(ParameterUtil.JOB_INFO)){
			info = (JobInfo) map.get(ParameterUtil.JOB_INFO);
		}
		if (ParameterUtil.ErrorCode.INPUT_PATH_NOTEXIST==(Integer) map.get(ParameterUtil.PATH_ERROR)  
				&& rule.getCron().length() > 3 && null!=info) {
			info.setEndTime(TimeUtil.formatFromUtcTime(new Date().getTime() + "",
					null));
			DataPersisManager.saveJobInfo(info);
		}
		
		if(ParameterUtil.ErrorCode.OUTPUT_PATH_EXIST==(Integer) map.get(ParameterUtil.PATH_ERROR)){//输出路径存在
			try{
				List<Map<String, String>> listMap=TailorUtil.getFailedTask(HiveMetaStore.getPatchPartitions(rule));
				if(listMap.size()>0){
					for(Map<String, String> m:listMap){
						if(m.toString().equals(rule.getPartitions().toString())){
							int result=HiveMetaStore.alterTable(rule);
							if(null!=info){
								info.setResult(result);
								DataPersisManager.saveJobInfo(info);
							}
							break;
						}
					}
				}
			}catch (Exception e) {
			}
		}
	}

	@Override
	public void jobToBeExecuted(JobExecutionContext context) {
	}

	@Override
	public void jobWasExecuted(JobExecutionContext context,
			JobExecutionException jobException) {
		JobDataMap map = context.getJobDetail().getJobDataMap();
		int result = map.getInt(ParameterUtil.JOB_RUN_RESULT);
		RuleEngine engine = (RuleEngine) map.get(ParameterUtil.RULE);
		JobInfo info = (JobInfo) map.get(ParameterUtil.JOB_INFO);
		info.setEndTime(TimeUtil.formatFromUtcTime(new Date().getTime() + "",
				null));
		Map<String,String> partitionMap=engine.getPartitions();
		info.setPartitions(partitionMap);
		if (result == 0) {
			logger.info("result is:"+result);
			result=HiveMetaStore.alterTable(engine);
		}
		info.setResult(result);
		try {
			logger.info(JaskSonUtil.getObjectMapper().writeValueAsString(info));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	    DataPersisManager.saveJobInfo(info);
	}

}
