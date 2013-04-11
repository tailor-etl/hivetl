package com.renren.tailor.parse.registry;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.renren.tailor.util.ParameterUtil;

public class PatternPool {
	public static final Map<String, Pattern> PATTERNS = new HashMap<String, Pattern>();

	static {
		PATTERNS.put("\\|", Pattern.compile("\\|"));
		PATTERNS.put(",", Pattern.compile(","));
		PATTERNS.put(ParameterUtil.PATTERN_NUMBER,
				Pattern.compile(ParameterUtil.PATTERN_NUMBER));
	}

	public static Pattern getPattern(String split) {
		if (PATTERNS.get(split) == null) {
			PATTERNS.put(split, Pattern.compile(split));
		}
		return PATTERNS.get(split);
	}
}
