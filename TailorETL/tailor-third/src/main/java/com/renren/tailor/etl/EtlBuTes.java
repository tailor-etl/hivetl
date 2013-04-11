package com.renren.tailor.etl;

import com.renren.tailor.parse.SpecialETLUDF;

public class EtlBuTes implements SpecialETLUDF {

	@Override
	public String invokeMethod(String method, String rawField, String rawLine) {
		if(method.equals("mybusiness")){
			return mybusiness(rawLine,rawField);
		}
		return "oh.no";
	}
	
	public String mybusiness(String rawLine,String rawField){
		
		return rawField;
	}

}
