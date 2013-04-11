package com.renren.tailor.parse;

import java.net.URL;
import java.util.List;

public class ParseUrlUDF implements ETLUDF {

	@Override
	public String evalute( String rawField,
			List<String> rules) {
		URL url;
		try {
			url = new URL(rawField);
		} catch (Exception e) {
			return "";
		}
		String partToExtract = rules.get(0);
		if (partToExtract.equals("HOST")) {
			return url.getHost();
		}
		if (partToExtract.equals("PATH")) {
			return url.getPath();
		}
		if (partToExtract.equals("QUERY")) {
			return url.getQuery();
		}
		if (partToExtract.equals("REF")) {
			return url.getRef();
		}
		if (partToExtract.equals("PROTOCOL")) {
			return url.getProtocol();
		}
		if (partToExtract.equals("FILE")) {
			return url.getFile();
		}
		if (partToExtract.equals("AUTHORITY")) {
			return url.getAuthority();
		}
		if (partToExtract.equals("USERINFO")) {
			return url.getUserInfo();
		}
		return "";
	}
	
//	public static void main(String[] args){
//		String ss="http://3g.renren.com/login.do?autoLogin=true&&fx=0";
//		ParseUrlUDF p=new ParseUrlUDF();
//		List<String> rules=new ArrayList<String>();
//		rules.add("HOST");
//		System.out.println(p.evalute(null, ss, rules));
//	}

}
