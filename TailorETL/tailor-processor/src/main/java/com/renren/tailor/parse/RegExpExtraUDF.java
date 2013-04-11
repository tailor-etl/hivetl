package com.renren.tailor.parse;

import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;

import com.renren.tailor.parse.registry.PatternPool;

public class RegExpExtraUDF implements ETLUDF {

	@Override
	public String evalute( String rawField,
			List<String> rules) {
		StringBuilder sb=new StringBuilder();
		Matcher m = PatternPool.getPattern(rules.get(0)).matcher(rawField);
		int extractIndex = 1;
		if (rules.size() > 1) {
			extractIndex = Integer.parseInt(rules.get(1));
		}
		if (m.find()) {
			if(extractIndex==-1){
					int i=1;
					while( i<=m.groupCount()){
						sb.append(m.group(i)+",");
					}
					if(sb.length()>0){
						sb.deleteCharAt(sb.length()-1);
					}
			}else{
				MatchResult mr = m.toMatchResult();
				sb.append( mr.group(extractIndex));
			}
		}
		return sb.toString();
	}

}
