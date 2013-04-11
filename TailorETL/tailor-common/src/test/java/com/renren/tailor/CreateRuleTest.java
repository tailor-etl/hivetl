package com.renren.tailor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.renren.tailor.model.RuleEngine;
import com.renren.tailor.model.RuleEngine.FieldRule;
import com.renren.tailor.util.JaskSonUtil;

public class CreateRuleTest {
	
	RuleEngine en=new RuleEngine();
	Map<String,String> partitions=new LinkedHashMap<String, String>();
	
	@Test
	public void creatTest() throws JsonGenerationException, JsonMappingException, IOException{
		partitions.put("log_date", "yyyy-MM-dd");
		en.setPartitions(partitions);
		List<Map<String,String>> ll=new ArrayList<Map<String,String>>();
		ll.add(partitions);
		List<LinkedHashMap<String, String>> partions = JaskSonUtil.getObjectMapper().readValue("[{\"log_date\":\"2013-04-01\"}]", List.class);
		System.out.println(partions);
//		en.setCron("10");
//        en.setName("ugcaction");
//		en.setTableName("ugcaction");
//		en.setInputPath("/warehouse/ugc_action_raw/log_date={yyyy-MM-dd}/{yyyy-MM-dd}.bz2");
//		en.setOutputPath("/user/xianbing.liu/tmp/ugcaction/");
//		en.setLength("4");
//		en.setSplit("\\|");
//		List<FieldRule> ll=new LinkedList<RuleEngine.FieldRule>();
//		ll.add(assemField("0", null, null, null));
//		ll.add(assemField("1", null, null, null));
//		ll.add(assemField("2", null, null, null));
//		Map<String, List<String>> ruleMaps=new HashMap<String, List<String>>();
//		List<String> ls=new LinkedList<String>();
//		ls.add("com.renren.tailor.etl.EtlBuTes");
//		ls.add("mybusiness");
//		ruleMaps.put("java_method", ls);
//		ll.add(assemField("3", null, ruleMaps, null));
//		en.setFieldRule(ll);
//		JaskSonUtil.getObjectMapper().writeValue(new File("ugcaction.json"), en);
	}
	
	public  FieldRule assemField(String i,String requires,Map<String, List<String>> ruleMaps,String replace){
		FieldRule fr=new FieldRule();
		fr.setFrom(i);
		if(StringUtils.isNotBlank(requires)){
			fr.setRequiredReg(requires);
		}
		fr.setRuleMaps(ruleMaps);
		fr.setReplaceStr(replace);
		return fr;
	}
}
