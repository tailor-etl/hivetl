package com.renren.tailor.util;

public interface ParameterUtil {
	
	interface ErrorCode{
		int INPUT_PATH_NOTEXIST=-101;
		int OUTPUT_PATH_EXIST=-102;
		
		int JOBRUNNING_ERROR=-20;
		
		int ALTER_TABLE_ERROR=-200;
	}
	
	String TIME_YEAR="yyyy";
	String TIME_YEAR_MONTH="yyyy-MM";
	String TIME_YEAR_MONTH_DAY="yyyy-MM-dd";
	String TIME_YEAR_MONTH_DAY_HOUR="hh";

	String JOB_DATA_COMMAND="command";
	String JOB_RUN_RESULT="result";//0:success
	String JOB_PARTITIONS="partitions";
	
	String JOB_RUNNING="JOB_RUNNING";
	String JOB_RUNNED="JOB_RUNNED";
	
	
	String PATTERN_NUMBER="^[0-9]*$";
	
	String USER_DEFINED_METHOD="java_method";
	
	String RULE_INFO="rule_info";
	
	String PATH_ERROR="path";
	
	String RULE="rule";
	
	String JOB_INFO="JOB_INFO";
	
	String FILE_INPUTPAT_NULL="FILE_INPUTPAT_NULL";
	
	String RESULT="result=";
	
	String JOBEXCEPTION="JOBEXCEPTION";
}
