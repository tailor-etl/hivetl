package com.renren.tailor.exec;



import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.renren.tailor.exception.ParseRuleException;
import com.renren.tailor.model.RuleEngine;
import com.renren.tailor.processor.DataConverter;
import com.renren.tailor.util.JaskSonUtil;
import com.renren.tailor.util.ParameterUtil;

public  class ExecMapper extends Mapper <LongWritable, Text, LongWritable, Text>  {

	private  RuleEngine ruleEngine;
  @Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		 try {
	    	Text t=	DataConverter.convert( value.toString(), ruleEngine);
	    	if(t!=null)
	        context.write(key, t);
	    } catch (InterruptedException e) {
	      e.printStackTrace();
	    } catch (ParseRuleException e) {
			e.printStackTrace();
		}
	}


@Override
protected  void setup(Context context) throws IOException, InterruptedException {
	super.setup(context);
	String name = context.getConfiguration().get(ParameterUtil.RULE_INFO);
	ruleEngine=	JaskSonUtil.getObjectMapper().readValue(name, RuleEngine.class);
}
  
}
