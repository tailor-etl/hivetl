package com.renren.tailor.exec;

import java.util.HashMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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

/**
 * lanuach mapreduce job mr job frame
 * 
 * @author chunguo.wang
 * 
 */
public class ExecDriver {

	private static Job job;
	private static Configuration conf = new Configuration();

	public static void main(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		HashMap<String, String> params = getJobParameters(otherArgs);
		execute(params.get(ParameterUtil.RULE), conf);
	}

	private static void execute(String rules, Configuration conf)
			throws Exception {
		RuleEngine rule = JaskSonUtil.getObjectMapper().readValue(rules,
				RuleEngine.class);
		try {
			conf.setBoolean("mapred.output.compress", true);
			conf.setClass("mapred.output.compression.codec", BZip2Codec.class,
					CompressionCodec.class);
			conf.set(ParameterUtil.RULE_INFO, rules);
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
			if (StringUtils.isNotBlank(rule.getReduceNum())) {
				job.setNumReduceTasks(Integer.valueOf(rule.getReduceNum()));
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
