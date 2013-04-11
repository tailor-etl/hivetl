package com.renren.tailor.parse;

import java.util.List;

import com.renren.tailor.parse.registry.PatternPool;

public class StrToMapUDF implements ETLUDF {

	@Override
	public String evalute(String rawField,
			List<String> rules) {
		String[] col = PatternPool.getPattern(rules.get(0)).split(rawField);
		String[] map = null;
		StringBuilder sb = new StringBuilder("{");
		if (col != null && col.length > 0) {
			for (String c : col) {
				map = PatternPool.getPattern(rules.get(1)).split(c);
				if (map != null && map.length == 2) {
					sb.append("\"" + map[0] + "\":\"" + map[1] + "\",");
				}
			}
		}
		if (sb.length() > 1) {
			return sb.substring(0, sb.length() - 1) + "}";
		}
		return "";
	}

}
