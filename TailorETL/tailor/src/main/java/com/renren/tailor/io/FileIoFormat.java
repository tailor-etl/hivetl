package com.renren.tailor.io;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.renren.tailor.exception.JobConfigExcepton;
import com.renren.tailor.model.RuleEngine;
import com.renren.tailor.util.HDFSUtil;
import com.renren.tailor.util.ParameterUtil;
import com.renren.tailor.util.TimeUtil;

/**
 * fileIO for job
 * 
 * @author chunguo.wang@renren-inc.com
 * 
 */
public class FileIoFormat {

	private static Log logger = LogFactory.getLog(FileIoFormat.class);

	private static String hdfsTempPath = "user/xianbing.liu/temp/tailor/";

//	public static Map<String, String> fileIO(RuleEngine rule, Job job)
//			throws JobConfigExcepton {
//		Map<String, String> patMap = rule.getPartitions();
//		Iterator<Entry<String, String>> it = patMap.entrySet().iterator();
//		String out = "";
//		Map<String, String> result = new LinkedHashMap<String, String>();
//		while (it.hasNext()) {
//			Entry<String, String> en = it.next();
//			result.put(en.getKey(),
//					TimeUtil.getLastInfo(en.getValue(), patMap.size()));
//			out += en.getKey() + "="
//					+ TimeUtil.getLastInfo(en.getValue(), patMap.size()) + "/";
//		}
//		out = out.substring(0, out.length() - 1);
//		logger.info(out);
//		String inputPath = TimeUtil.getInputPath(rule.getInputPath());
//
//		Path[] inPaths = null;
//		if (inputPath.indexOf(",") > 0) {
//			String[] ip = inputPath.split(",");
//			inPaths = new Path[ip.length];
//			int i = 0;
//			for (String p : ip) {
//				inPaths[i++] = new Path(p);
//				checkFileInputPath(p, rule);
//			}
//		} else {
//			checkFileInputPath(inputPath, rule);
//			inPaths = new Path[1];
//			inPaths[0] = new Path(inputPath);
//		}
//		Path outPath=new Path(rule.getOutputPath()+ out);
//		try {
//			if(HDFSUtil.getFileSystem().exists(outPath)){
//				throw new JobConfigExcepton("file not exist", ParameterUtil.ErrorCode.OUTPUT_PATH_EXIST);
//			}
//		} catch (IOException e1) {
//			e1.printStackTrace();
//		}
//		
//		
//		try {
//			FileInputFormat.setInputPaths(job, inPaths);
//			FileOutputFormat.setOutputPath(job, outPath);
//			FileOutputFormat.setCompressOutput(job, true);
//		} catch (IOException e) {
//			e.printStackTrace();
//			throw new JobConfigExcepton("set file path error", e);
//		}
//        return result;
//	}

	private static boolean checkFileInputPath(String inputPath, RuleEngine rule)
			throws JobConfigExcepton {
		if (rule.isLocal()) {
			// File file = new File(inputPath);
			// String remotePath = "";
			// if(!file.exists()){
			// if(rule.getCron().length()<=3 ){//循环判断文件
			// return false;
			// }else{
			// throw new JobConfigExcepton("file not exist...");
			// }
			// }else{
			// remotePath = hdfsTempPath + file.getName();
			// try {
			// HDFSUtil.putLocalFileToHdfs(inputPath, remotePath);
			// } catch (IOException e) {
			// e.printStackTrace();
			// throw new JobConfigExcepton("upload file error", e);
			// }
			// }

		} else {
			FileSystem fs = HDFSUtil.getFileSystem();
			// if(rule.getCron().length()<=3){//如果是定时查看文件
			// try {
			// if((fs==null || !fs.exists(new Path(inputPath)))){
			// return false;
			// }
			// } catch (IOException e) {
			// e.printStackTrace();
			// return false;
			// }
			// }else{
			try {
				if ((fs == null || !fs.exists(new Path(inputPath)))) {
					throw new JobConfigExcepton("file not exist", ParameterUtil.ErrorCode.INPUT_PATH_NOTEXIST);
				}
			} catch (IOException e) {
				e.printStackTrace();
				throw new JobConfigExcepton("file not exist", ParameterUtil.ErrorCode.INPUT_PATH_NOTEXIST);
			}
			// }

		}
		return true;
	}
}
