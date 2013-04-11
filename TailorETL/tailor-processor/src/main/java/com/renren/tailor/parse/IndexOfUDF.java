package com.renren.tailor.parse;

import java.util.List;

public class IndexOfUDF implements ETLUDF {

	@Override
	public String evalute( String rawField,
			List<String> rules)  {
//		if (null == rules || rules.size() ==0)
//			throw new IllegalArgumentException("rule size error");
		if(rules.size()==1){
			return rawField.indexOf(rules.get(0))+"";
		}
		return rawField.indexOf(rules.get(0), Integer.parseInt(rules.get(1)))+"";
	}

}
