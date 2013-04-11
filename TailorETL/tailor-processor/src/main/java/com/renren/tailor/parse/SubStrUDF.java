package com.renren.tailor.parse;

import java.util.List;

public class SubStrUDF implements ETLUDF {

	@Override
	public String evalute( String rawField,
			List<String> rules) {
		int[] index = null;
		int start = Integer.parseInt(rules.get(0)), end = 0;
		if (rules.size() > 1) {
			end = Integer.parseInt(rules.get(1));
		} else {
			end = rawField.length();
		}
		if (rawField.length() <= 0 || end <= 0) {
			return "";
		}
		index = makeIndex(start, end, rawField.length());
		if (index == null) {
			return "";
		}
		return rawField.substring(index[0], index[1]);
	}

	private int[] makeIndex(int pos, int len, int inputLen) {
		int[] index = new int[2];
		if ((Math.abs(pos) > inputLen)) {
			return null;
		}
		int start, end;
		if (pos > 0) {
			start = pos - 1;
		} else if (pos < 0) {
			start = inputLen + pos;
		} else {
			start = 0;
		}
		if ((inputLen - start) < len) {
			end = inputLen;
		} else {
			end = start + len;
		}
		index[0] = start;
		index[1] = end;
		return index;
	}

}
