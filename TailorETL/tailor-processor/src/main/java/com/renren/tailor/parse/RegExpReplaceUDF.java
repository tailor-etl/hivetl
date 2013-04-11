package com.renren.tailor.parse;

import java.util.List;
import java.util.regex.Matcher;

import com.renren.tailor.parse.registry.PatternPool;

public class RegExpReplaceUDF implements ETLUDF {

	@Override
	public String evalute( String rawField,
			List<String> rules) {
		StringBuffer sb = new StringBuffer();
//		if (null == rules || rules.size() < 2)
//			throw new IllegalArgumentException("rule size error");
//		if (patternMap.get(rules.get(0)) == null) {
//			patternMap.put(rules.get(0), Pattern.compile(rules.get(0)));
//		}
		Matcher m = PatternPool.getPattern(rules.get(0)).matcher(rawField);
		while (m.find()) {
			m.appendReplacement(sb, rules.get(1));
		}
		m.appendTail(sb);
		return sb.toString();
	}

}
