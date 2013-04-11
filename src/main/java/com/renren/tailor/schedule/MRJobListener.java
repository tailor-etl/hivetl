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
		if (map.get(ParameterUtil.PATH_ERROR) != null
				&& (Integer) map.get(ParameterUtil.PATH_ERROR) == ParameterUtil.ErrorCode.INPUT_PATH_NOTEXIST
				&& rule.getCron().length() > 3) {
			JobInfo info = (JobInfo) map.get(ParameterUtil.JOB_INFO);
			info.setEndTime(TimeUtil.formatFromUtcTime(new Date().getTime() + "",
					null));
			DataPersisManager.saveJobInfo(info);
		}
	}

	@Override
	public void jobToBeExecuted(JobExecutionContext context) {
		// JobDataMap map = context.getMergedJobDataMap();
		// String[] command = (String[])
		// map.get(ParameterUtil.JOB_DATA_COMMAND);
		// String rul="-"+ParameterUtil.RULE;
		// map.put(ParameterUtil.RULE,
		// command[2].substring(command[2].indexOf(rul)+rul.length()+1));
		// RuleEngine
		// rule=JaskSonUtil.readValueAsObjFromStr(map.getString(ParameterUtil.RULE),
		// RuleEngine.class);
		//
		// Map<String, String> patMap = rule.getPartitions();
		// Iterator<Entry<String, String>> it = patMap.entrySet().iterator();
		// String out = "";
		// Map<String, String> result = new LinkedHashMap<String, String>();
		// while (it.hasNext()) {
		// Entry<String, String> en = it.next();
		// result.put(en.getKey(),
		// TimeUtil.getLastInfo(en.getValue(), patMap.size()));
		// out += en.getKey() + "="
		// + TimeUtil.getLastInfo(en.getValue(), patMap.size()) + "/";
		// }
		// out = out.substring(0, out.length() - 1);
		// rule.setOutputPath(rule.getOutputPath()+ out);
		// rule.setPartitions(result);
		// rule.setInputPath(TimeUtil.getInputPath(rule.getInputPath()));
		//
		// try {
		// boolean flag=DataPersisManager.checkJobRunned(rule.getTableName(),
		// JaskSonUtil.getObjectMapper().writeValueAsString(result), 0);
		// map.put(ParameterUtil.JOB_RUNNED, flag);
		// } catch (JsonProcessingException e) {
		// e.printStackTrace();
		// }
		// //String inputPath = TimeUtil.getInputPath(rule.getInputPath());
		//
		// if(RUNNING_JOB.get(rule.getTableName())!=null){//有作业正在运行
		// map.put(ParameterUtil.JOB_RUNNING, true);
		// logger.info("上一个作业正在运行..."+rule.getTableName());
		// }else{
		// map.put(ParameterUtil.JOB_RUNNING, false);
		// RUNNING_JOB.put(rule.getTableName(), rule.getTableName());
		// }
		// JobInfo info = new JobInfo();
		// info.setStartTime(TimeUtil.formatFromUtcTime(new
		// Date().getTime()+"",null));
		// info.setPartitions(result);
		// map.put(ParameterUtil.JOB_INFO, info);
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
//			Iterator<Entry<String, String>> it = partitionMap.entrySet()
//					.iterator();
//			String sql = "alter table " + engine.getTableName()
//					+ " add partition (";
//			List<String> list = new ArrayList<String>();
//			while (it.hasNext()) {
//				Entry<String, String> en = it.next();
//				list.add(en.getKey() + "=" + en.getValue());
//				sql += en.getKey() + "='" + en.getValue() + "',";
//			}
//			sql = sql.substring(0, sql.length() - 1);
//			String location = "";
//			for (String s : list) {
//				location += s + "/";
//			}
//			location = location.substring(0, location.length() - 1);
//			sql += ") location '" + engine.getOutputPath() + location + "'";
//			logger.info(sql);
//			try {
//				HiveMetaStore.alterPartition(sql);
//			} catch (SQLException e) {
//				e.printStackTrace();
//				logger.error(e.getMessage() + " alter table error " + sql);
//				result = ParameterUtil.ErrorCode.ALTER_TABLE_ERROR;
//			}
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
