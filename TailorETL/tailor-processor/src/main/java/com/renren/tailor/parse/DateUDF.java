package com.renren.tailor.parse;

import java.util.List;

import com.renren.tailor.util.TimeUtil;

public class DateUDF implements ETLUDF {

	public String evalute(String rawLine,
			List<String> rules) {
		String type=rules.get(0);
		String format=null;
		if(rules.size()>1){
			format=rules.get(1);
		}
		if(type.equals("unix_timestamp")){
			return TimeUtil.formatToUnixTime(rawLine, format)+"";
		}else if(type.equals("utc_timestamp")){
			return TimeUtil.formatToUtcTime(rawLine, format)+"";
		}else if(type.equals("to_date")){
			String res="";
			String timeType=null;
			if(rules.size()>2)timeType=rules.get(2);
			if(timeType==null)res=TimeUtil.formatToDate(rawLine);
			if(timeType.equals("utc"))res=TimeUtil.formatFromUtcTime(rawLine, format);
			if(timeType.equals("unix"))res=TimeUtil.formatFromUnixTime(rawLine, format);
			return res;
		}
		return null;
	}


}
