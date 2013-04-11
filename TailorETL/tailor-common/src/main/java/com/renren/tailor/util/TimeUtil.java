package com.renren.tailor.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class TimeUtil {
	
	public static final SimpleDateFormat formatter_date = new SimpleDateFormat("yyyy-MM-dd");
	
	public static final SimpleDateFormat formatter_year = new SimpleDateFormat("yyyy");
	
	public static final SimpleDateFormat formatter_month = new SimpleDateFormat("yyyy-MM");

	public static final SimpleDateFormat formatter_second = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
	private static final Calendar calendar = Calendar.getInstance();
	
	
	public static String formatToDate(String str) {
		try {
			 Date date = formatter_date.parse(str);
			return formatter_date.format(date);
		} catch (Exception e) {
			return null;
		}
	}

	
	public static int formatToSecond(String str, String format) {
		SimpleDateFormat formatter;
		if (StringUtils.isEmpty(format)) {
			formatter = formatter_second;
		} else {
			formatter = new SimpleDateFormat(format);
		}
		try {
			Date date = formatter.parse(str);
			calendar.setTime(date);
			return calendar.get(Calendar.SECOND);
		} catch (ParseException e) {
			return 0;
		}
	}

	public static int formatToMinute(String str, String format) {
		SimpleDateFormat formatter;
		if (StringUtils.isEmpty(format)) {
			formatter = formatter_second;
		} else {
			formatter = new SimpleDateFormat(format);
		}
		try {
			Date date = formatter.parse(str);
			calendar.setTime(date);
			return calendar.get(Calendar.MINUTE);
		} catch (ParseException e) {
			return 0;
		}
	}
	
	public static int formatToHour(String str, String format) {
		SimpleDateFormat formatter;
		if (StringUtils.isEmpty(format)) {
			formatter = formatter_second;
		} else {
			formatter = new SimpleDateFormat(format);
		}
		try {
			Date date = formatter.parse(str);
			calendar.setTime(date);
			return calendar.get(Calendar.HOUR_OF_DAY);
		} catch (ParseException e) {
			return 0;
		}
	}

	public static String formatFromUnixTime(String str, String format) {
		SimpleDateFormat formatter;
		if (StringUtils.isEmpty(format)) {
			formatter = formatter_second;
		} else {
			formatter = new SimpleDateFormat(format);
		}
		try {
			long unixtime = Long.parseLong(str);
			Date date = new Date(unixtime * 1000L);
			return formatter.format(date);
		} catch (Exception e) {
			return null;
		}
	}

	public static String formatFromUtcTime(String str, String format) {
		SimpleDateFormat formatter;
		if (StringUtils.isEmpty(format)) {
			formatter = formatter_second;
		} else {
			formatter = new SimpleDateFormat(format);
		}
		try {
			long unixtime = Long.parseLong(str);
			Date date = new Date(unixtime);
			return formatter.format(date);
		} catch (Exception e) {
			return null;
		}
	}

	public static long formatToUtcTime(String str, String format) {
		SimpleDateFormat formatter;
		if (StringUtils.isEmpty(format)) {
			formatter = formatter_second;
		} else {
			formatter = new SimpleDateFormat(format);
		}
		try {
			return formatter.parse(str).getTime();
		} catch (ParseException e) {
			return 0l;
		}
	}

	public static long formatToUnixTime(String str, String format) {
		SimpleDateFormat formatter;
		if (StringUtils.isEmpty(format)) {
			formatter = formatter_second;
		} else {
			formatter = new SimpleDateFormat(format);
		}
		try {
			return formatter.parse(str).getTime() / 1000;
		} catch (ParseException e) {
			return 0l;
		}
	}
	
	public static String getLastInfo(String str,int size){
		if(str.equals(ParameterUtil.TIME_YEAR_MONTH_DAY)||str.equals(ParameterUtil.TIME_YEAR_MONTH_DAY_HOUR)){
			if(str.length()<=2){
				return getLastHour()+"";
			}else{
				if(size==2){
					return getNowDay();
				}else{
					return getLastDay();
				}
			}
		}else{
			return str;
		}
	}
	public static String getNowDay(){
		Date yes = new Date(System.currentTimeMillis());
		return formatter_date.format(yes);
	}
	public static String getLastDay(){
		Date yes = new Date(System.currentTimeMillis() - 1000 * 60 * 60 * 24);
		return formatter_date.format(yes);
	}
	public static String getLastDayOfYear(){
		Date yes = new Date(System.currentTimeMillis() - 1000 * 60 * 60 * 24);
		return formatter_year.format(yes);
	}
	public static int getLastHour(){
		Date date=new Date(System.currentTimeMillis()-  1000 * 60 * 60);
		calendar.setTime(date);
		return calendar.get(Calendar.HOUR_OF_DAY);
	}
	
	public static String getInputPath(String path,LinkedList<String> list){
		if(!list.contains(ParameterUtil.TIME_YEAR_MONTH_DAY) && !list.contains(ParameterUtil.TIME_YEAR_MONTH_DAY_HOUR)){
			path=path.replace("{"+ParameterUtil.TIME_YEAR_MONTH_DAY+"}",list.get(0));
			if(list.size()>1){
				path=path.replace("{"+ParameterUtil.TIME_YEAR_MONTH_DAY_HOUR+"}",list.get(1));
			}
		}else{
			path=path.replace("{"+ParameterUtil.TIME_YEAR_MONTH_DAY+"}", getLastDay()).
			replace("{"+ParameterUtil.TIME_YEAR+"}", getLastDayOfYear()).replace("{"+ParameterUtil.TIME_YEAR_MONTH_DAY_HOUR+"}", getLastHour()+"");
		}
		return path;
	}

	public static void main(String[] args) throws ParseException {
	}
}
