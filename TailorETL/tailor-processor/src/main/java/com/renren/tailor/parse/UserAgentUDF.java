package com.renren.tailor.parse;

import java.util.List;

import nl.bitwalker.useragentutils.UserAgent;

public class UserAgentUDF implements ETLUDF {

	@Override
	public String evalute(String rawLine,
			List<String> rules) {
		UserAgent ua = UserAgent.parseUserAgentString(rawLine);
		StringBuilder sb=new StringBuilder();
		String split=",";
		if(rules.size()>1){
			if(rules.size()>2){
				split=rules.get(2);
			}
			sb.append(ua.getBrowser().toString() + "/" + ua.getBrowserVersion()+split+ua.getOperatingSystem().toString());
		}else{
			String type = rules.get(0);
			if (type.equals("browser")) {
				sb.append( ua.getBrowser().toString() + "/" + ua.getBrowserVersion());
			} else if (type.equals("os")) {
				sb.append( ua.getOperatingSystem().toString());
			}
		}
		return sb.toString();
	}

}
