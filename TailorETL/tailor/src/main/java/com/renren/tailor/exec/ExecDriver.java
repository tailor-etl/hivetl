package com.renren.tailor.exec;

import java.util.HashMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.renren.tailor.model.RuleEngine;
import com.renren.tailor.util.JaskSonUtil;
import com.renren.tailor.util.ParameterUtil;
import com.renren.tailor.util.PropertiesUtil;

/**
 * lanuach mapreduce job mr job frame
 * 
 * @author chunguo.wang
 * 
 */
public class ExecDriver {

	private static Log logger = LogFactory.getLog(ExecDriver.class) ;
	
	private static Job job;
	private static RuleEngine rule ;
	private static Configuration conf = new Configuration();

	public static void main(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		for(String s:otherArgs){
			logger.info("loop string:"+s);
		}
		HashMap<String, String> params = getJobParameters(otherArgs);
		logger.info("message:"+params.get(ParameterUtil.RULE)+"#end");
		 rule = JaskSonUtil.getObjectMapper().readValue(params.get(ParameterUtil.RULE),
				RuleEngine.class);
		conf.set(ParameterUtil.RULE_INFO, params.get(ParameterUtil.RULE));
		if(rule.getFieldRule()==null || rule.getFieldRule().size()==0){//如果没有对列过滤的需求
		}else{
			execute();
		}
	}

	private static void execute()
			throws Exception {
		try {
			conf.setStrings("fs.default.name", rule.getHdfs());
		    conf.setStrings(FileSystem.FS_DEFAULT_NAME_KEY,rule.getHdfs());
			conf.setBoolean("mapred.output.compress", true);
			conf.setClass("mapred.output.compression.codec", BZip2Codec.class,
					CompressionCodec.class);
			
			Cluster cluster = new Cluster(conf);
			job = Job.getInstance(cluster, conf);

			job.setJarByClass(ExecDriver.class);
			job.setJobName(rule.getName());

			job.setMapperClass(ExecMapper.class);
			job.setReducerClass(ExecReducer.class);

			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			if (rule.getReduceNum()>0) {
				job.setNumReduceTasks(rule.getReduceNum());
			}
			Path[] inPaths = null;
			String[] ip = rule.getInputPath().split(",");
			inPaths = new Path[ip.length];
			int i = 0;
			for (String p : ip) {
					inPaths[i++] = new Path(p);
			}
			FileInputFormat.setInputPaths(job, inPaths);
			FileOutputFormat.setOutputPath(job, new Path(rule.getOutputPath()));
			FileOutputFormat.setCompressOutput(job, true);
			
			job.waitForCompletion(true);
		} catch (Throwable e) {
			e.printStackTrace();
			throw new Exception(e.getMessage(), e);
		}
	}

	private static HashMap<String, String> getJobParameters(String[] args) {
		Options options = new Options();
		for (String arg : args) {
			if (arg.startsWith("-")) {
				@SuppressWarnings("static-access")
				Option option = OptionBuilder.hasArg(true).create(
						arg.substring(1));
				option.setArgName(arg.substring(1));
				options.addOption(option);
			}
		}
		PosixParser posixerParser = new PosixParser();
		CommandLine cmd = null;
		HashMap<String, String> params = null;
		try {
			params = new HashMap<String, String>();
			cmd = posixerParser.parse(options, args);
			for (Option op : cmd.getOptions()) {
				params.put(op.getArgName(), op.getValue());
			}
		} catch (org.apache.commons.cli.ParseException e) {
			e.printStackTrace();
		}
		return params;
	}
}
