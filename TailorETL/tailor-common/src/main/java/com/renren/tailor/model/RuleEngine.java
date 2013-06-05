package com.renren.tailor.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonAutoDetect
@JsonInclude(Include.NON_NULL)
public class RuleEngine implements Serializable {

	/**
   * 
   */
	private static final long serialVersionUID = 7033595218173055954L;

	private String cron;//任务执行的时间表达式 如果cron为数字类型 那么就是轮询某个目录
	private String name;//规则名字
	private String inputPath;//输入路径,多个以逗号分割  如果为null，那么直接入hive
	private String outputPath;//输出路径
	private String split;//原始文件字段分割字符
	private String createTime;//规则文件创建时间
	private int reduceNum=10;//设置reduce数量
	private boolean isLocal=false;//文件是否是本地
	private Map<String, List<String>> globalRules;//针对原始数据一行进行转换
	private String length;//字段数量限制
	private String partitionFormat;// 输出的文件目录名称格式 如tail={yyyy-MM-dd}
	private Map<String, String> partitions;// 分区信息 key：log_date value:2013-03-12
	private String tableName;// hive表名称
	private String dbName;//库名称
	private String extJarName;
	private boolean isContinute;//表分区是否按照时间顺序
	private String hdfs="hdfs://BJCER256-230.opi.com";//集群 默认是塞尔机器
	@JsonInclude(Include.NON_NULL)
	public static class FieldRule {
		private String from;//原始字段下标位置索引 如果多个字段组合 那么以逗号分割 如2,3,4
		private String fixValue;//新字段固定的值
		private String requiredReg;//需要验证的正则
		private String replaceStr;//需要替换的字符
		private Map<String, List<String>> ruleMaps;//对字段执行相应的规则，key为规则名称，value为参数

		public String getFixValue() {
			return fixValue;
		}

		public void setFixValue(String fixValue) {
			this.fixValue = fixValue;
		}
		public String getReplaceStr() {
			return replaceStr;
		}

		public void setReplaceStr(String replaceStr) {
			this.replaceStr = replaceStr;
		}

		public String getRequiredReg() {
			return requiredReg;
		}

		public void setRequiredReg(String requiredReg) {
			this.requiredReg = requiredReg;
		}

		public Map<String, List<String>> getRuleMaps() {
			return ruleMaps;
		}

		public void setRuleMaps(Map<String, List<String>> ruleMaps) {
			this.ruleMaps = ruleMaps;
		}

		public String getFrom() {
			return from;
		}
		public void setFrom(String from) {
			this.from = from;
		}

	}
	
	public String getExtJarName() {
		return extJarName;
	}

	public void setExtJarName(String extJarName) {
		this.extJarName = extJarName;
	}
	
	public boolean isLocal() {
		return isLocal;
	}

	public void setLocal(boolean isLocal) {
		this.isLocal = isLocal;
	}
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Map<String, String> getPartitions() {
		return partitions;
	}

	public void setPartitions(Map<String, String> partitions) {
		this.partitions = partitions;
	}

	private List<FieldRule> fieldRule;


	public List<FieldRule> getFieldRule() {
		return fieldRule;
	}

	public void setFieldRule(List<FieldRule> fieldRule) {
		this.fieldRule = fieldRule;
	}

	public Map<String, List<String>> getGlobalRules() {
		return globalRules;
	}

	public void setGlobalRules(Map<String, List<String>> globalRules) {
		this.globalRules = globalRules;
	}

	public String getName() {
		return name;
	}
	
	public String getHdfs() {
		return hdfs;
	}

	public void setHdfs(String hdfs) {
		this.hdfs = hdfs;
	}

	public String getPartitionFormat() {
		return partitionFormat;
	}

	public void setPartitionFormat(String partitionFormat) {
		this.partitionFormat = partitionFormat;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getSplit() {
		return split;
	}

	public void setSplit(String split) {
		this.split = split;
	}

	public String getCreateTime() {
		return createTime;
	}

	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}

	public int getReduceNum() {
		return reduceNum;
	}

	public void setReduceNum(int reduceNum) {
		this.reduceNum = reduceNum;
	}

	public String getLength() {
		return length;
	}

	public void setLength(String length) {
		this.length = length;
	}

	public String getCron() {
		return cron;
	}

	public void setCron(String cron) {
		this.cron = cron;
	}
	public boolean isContinute() {
		return isContinute;
	}
	public void setContinute(boolean isContinute) {
		this.isContinute = isContinute;
	}
	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}


}
