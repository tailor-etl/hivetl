package com.renren.tailor.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HTTP;

public class TailorUtil {
	
	 private static Log logger = LogFactory.getLog(TailorUtil.class);

	 public final static String BaseEmailURL = "http://email.notify.d.xiaonei.com:4001/eml/";

	  public static boolean postEmail(String title, String email, String content) throws ClientProtocolException,
	      IOException {
	    String url = BaseEmailURL + title + "/" + email + "?fromadd=genome";
	    HttpPost httppost = new HttpPost(url);
	    httppost.setEntity(new StringEntity(content, HTTP.UTF_8));
	    HttpResponse response = new DefaultHttpClient().execute(httppost);
	    if (response.getStatusLine().getStatusCode() == 200) {// 如果状态码为200,就是正常返回
	      return true;
	    }
	    return false;
	  }
	  
	public static List<Map<String, String>> getFailedTask(List<String> list) {
		int size = list.size();
		List<Map<String, String>> mapList = new ArrayList<Map<String, String>>();
		Map<String, String> map;
		String[] spli;
		if (size > 0 && list.get(0).indexOf("/") > 0) {
			for (int i = 0; i < size; i++) {
				String[] arr = list.get(i).split("/");
				map = new LinkedHashMap<String, String>();
				for (String s : arr) {
					spli = s.split("=");
					map.put(spli[0], spli[1]);
				}
				mapList.add(map);
			}
		} else {
			for (int i = 0; i < size; i++) {
				spli = list.get(i).split("=");
				map = new LinkedHashMap<String, String>();
				map.put(spli[0], spli[1]);
				mapList.add(map);
			}
		}
		List<Map<String, String>> res = new ArrayList<Map<String, String>>();
		int k = mapList.size();
		for (int i = 0; i < k - 1; i++) {
			findUnRunDate(mapList.get(i), mapList.get(i + 1), res);
		}
		logger.info("norunn:"+res.toString());
		return res;
	}

	public static void findUnRunDate(Map<String, String> begin,
			Map<String, String> end, List<Map<String, String>> res) {
		TreeSet<String> keySet = new TreeSet<String>(begin.keySet());
		String beginDate, endDate;
		List<String> list;
		if (begin.size() == 1) {// 按照天分区
			beginDate = begin.get(keySet.first());
			endDate = end.get(keySet.first());
			list = TimeUtil.getDatePhrase(beginDate, endDate);
			for (String s : list) {
				Map<String, String> result = new LinkedHashMap<String, String>();
				result.put(keySet.first(), s);
				res.add(result);
			}
		} else if (begin.size() == 2) {// 按照天 小时 分区
			beginDate = begin.get(keySet.first()) + "-"
					+ begin.get(keySet.last());
			endDate = end.get(keySet.first()) + "-" + end.get(keySet.last());
			list = TimeUtil.getDatePhrase(beginDate, endDate);
			for (String s : list) {
				int ind = s.lastIndexOf("-");
				Map<String, String> result = new LinkedHashMap<String, String>();
				result.put(keySet.first(), s.substring(0, ind));
				result.put(keySet.last(), s.substring(ind + 1));
				res.add(result);
			}
		}
	}
}
