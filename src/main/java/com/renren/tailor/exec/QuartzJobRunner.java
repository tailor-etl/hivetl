package com.renren.tailor.exec;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.renren.tailor.util.ParameterUtil;

public class QuartzJobRunner implements Job {

	private static Log logger = LogFactory.getLog(QuartzJobRunner.class);

	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		JobDataMap map = context.getJobDetail().getJobDataMap();
		String[] engine = (String[]) map.get(ParameterUtil.JOB_DATA_COMMAND);
		Process process;
		int result = -1;
		try {
			process = Runtime.getRuntime().exec(engine);
			printInfo(process.getErrorStream());
			printInfo(process.getInputStream());
			result = process.waitFor();
		} catch (Exception e1) {
			e1.printStackTrace();
			logger.error("execute job error"+e1.getMessage());
		}
		map.put(ParameterUtil.JOB_RUN_RESULT, result);
	}

	private void printInfo(InputStream inputstream) {
		BufferedReader br = null;
		InputStreamReader isr = null;
		try {
			isr = new InputStreamReader(inputstream);
			br = new BufferedReader(isr);
			String line = null;
			while ((line = br.readLine()) != null) {
					logger.info(line);
			}
		} catch (Exception e) {
		} finally {
			try {
				br.close();
				isr.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
