package com.renren.tailor.parse.registry;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.renren.tailor.exception.ParseRuleException;
import com.renren.tailor.parse.ETLUDF;
import com.renren.tailor.parse.SpecialETLUDF;
import com.renren.tailor.util.PropertiesUtil;

@SuppressWarnings("rawtypes")
public final class FunctionRegistry {

	public static final Map<String,ETLUDF> ETLFUNCTIONS=new HashMap<String,ETLUDF>();
	
	public static final Map<String,SpecialETLUDF> SPECIALUDF=new ConcurrentHashMap<String, SpecialETLUDF>();
	
	public static SpecialETLUDF getSpecialETLUDF(String className) throws ParseRuleException{
		if(SPECIALUDF.get(className)==null){
			try {
				Class cl=Class.forName(className);
				SPECIALUDF.put(className, (SpecialETLUDF) cl.newInstance());
			} catch (Exception e) {
				e.printStackTrace();
				throw new ParseRuleException("class not found");
			} 
		}
		return SPECIALUDF.get(className);
	}
	
	
	static{
		Properties pro=PropertiesUtil.getProperties("/conf/system_function.properties", FunctionRegistry.class);
		Set<Entry<Object, Object>> set = pro.entrySet();
		String value="";
		for (Entry<Object, Object> en : set) {
			value = (String) en.getValue();
			try {
				value = new String(value.getBytes("ISO8859-1"), "utf-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			value = value.substring(value.indexOf(",")+1);
			Class cl;
			Object obj;
			try {
				cl = Class.forName(value);
				 obj=cl.newInstance();
				ETLFUNCTIONS.put((String) en.getKey(), (ETLUDF)obj);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		System.out.println(ETLFUNCTIONS);
	}
	
	public static void main(String[] args){
		System.out.println("aaa"+ETLFUNCTIONS);
	}
}
