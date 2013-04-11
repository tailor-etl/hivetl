package com.renren.tailor.processor;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import com.renren.tailor.exception.ParseRuleException;
import com.renren.tailor.model.RuleEngine;
import com.renren.tailor.model.RuleEngine.FieldRule;
import com.renren.tailor.parse.registry.FunctionRegistry;
import com.renren.tailor.parse.registry.PatternPool;
import com.renren.tailor.util.ParameterUtil;

public final class DataConverter {

	private static final Log logger = LogFactory.getLog(DataConverter.class);
	
	public static  Text convert(String rawLine,RuleEngine rule) throws ParseRuleException{
		if(StringUtils.isEmpty(rawLine))return null;
		if(rule.getGlobalRules()!=null && rule.getGlobalRules().size()>0){//行数据转换
			rawLine=convertUseFunction(rule.getGlobalRules().entrySet().iterator(),rawLine,rawLine);
		}
		if(StringUtils.isEmpty(rawLine))return null;
		
		String split = rule.getSplit();
		String[] arrays=PatternPool.getPattern(split).split(rawLine);
		if(StringUtils.isNotBlank(rule.getLength()) && arrays.length<Integer.parseInt(rule.getLength()) ){
			return null;
		}
		List<FieldRule> ruleList=rule.getFieldRule();
//		if(null!=ruleList && Integer.parseInt(ruleList.get(ruleList.size()-1).getFrom())>=arrays.length){
//			throw new ParseRuleException("parse error");
//		}
		String rawField=null;
		StringBuilder sb=new StringBuilder();
		String mysplit=split;
		if(split.startsWith("\\")){
			mysplit=split.substring(1);
		}
		//遍历需要check的字段
		for(FieldRule fu:ruleList){
			if(StringUtils.isNotBlank(fu.getFrom()) && StringUtils.isNotBlank(fu.getRequiredReg())){
				if(fu.getFrom().contains(",")){
					String[] arr=PatternPool.getPattern(",").split(fu.getFrom());
					for(String s:arr){
						rawField+=arrays[Integer.parseInt(s)];
					}
				}else{
					rawField=arrays[Integer.parseInt(fu.getFrom())];
				}
				 if(!PatternPool.getPattern(fu.getRequiredReg()).matcher(rawField).matches() && null==fu.getFixValue()){
							return null;
				  }
			}
		}
		
		//多原始字段进行规则清理
		for(FieldRule fu:ruleList){
			if(fu.getFixValue()!=null){
				rawField=fu.getFixValue();
			}else{
				if(StringUtils.isNotBlank(fu.getFrom())){
					if(fu.getFrom().contains(",")){
						String[] arr=PatternPool.getPattern(",").split(fu.getFrom());
						for(String s:arr){
							rawField+=arrays[Integer.parseInt(s)];
						}
					}else{
						rawField=arrays[Integer.parseInt(fu.getFrom())];
					}
					if(fu.getRuleMaps()!=null){
						Iterator<Entry<String, List<String>>> it=fu.getRuleMaps().entrySet().iterator();
						rawField=convertUseFunction(it,rawField,rawLine);
					 }
				}
			}
			sb.append(mysplit+rawField);
		}
		if(sb.length()>0){
			return new Text(sb.substring(mysplit.length()).toString());
		}
		return null;
	}
	
	private static String convertUseFunction(Iterator<Entry<String, List<String>>> it,String rawStr,String rawLine) throws ParseRuleException{
		while(it.hasNext()){
			Entry<String, List<String>> en=it.next();
			if(en.getKey().equals(ParameterUtil.USER_DEFINED_METHOD)){
				List<String> ll=en.getValue();
				if(null==ll || ll.size()!=2){
					throw new ParseRuleException("java_method parameters error");
				}
				rawStr=FunctionRegistry.getSpecialETLUDF(ll.get(0)).invokeMethod(ll.get(1),rawStr, rawLine);
			}else{
				try {
					rawStr=FunctionRegistry.ETLFUNCTIONS.get(en.getKey()).evalute(rawStr, en.getValue());
				}catch (RuntimeException e) {
					logger.error("runtimeexception hadppend");
					rawStr="";
					break;
				}
			}
		}
		return rawStr;
	}
	
}
