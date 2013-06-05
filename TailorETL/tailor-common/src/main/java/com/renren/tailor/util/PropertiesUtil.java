package com.renren.tailor.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class PropertiesUtil {

  private final static Log logger = LogFactory.getLog(PropertiesUtil.class.getName());
  
  public final static Configuration conf=new Configuration();
  /**
   * @param absolutePath
   *          请给出在jar文件中的绝对路径,即以“/”开头。
   * @return
   */
  @SuppressWarnings("rawtypes")
public static Properties getProperties(String absolutePath,Class cla) {
    Properties props = new Properties();
    try {
      props.load(cla.getResourceAsStream(absolutePath));
    } catch (IOException e) {
      logger.error("can not load the property file: " + absolutePath + " relative to this class");
    }
    return props;
  }
  
  
  public static void loadProp(String fileName) throws IOException{
		Properties prop = new Properties();
		InputStream is = PropertiesUtil.class.getClassLoader()
				.getResourceAsStream(fileName);

		prop.load(is);
		
		Set<Entry<Object,Object>> set=prop.entrySet();
		for(Entry<Object,Object> en:set){
			conf.set(en.getKey().toString(), en.getValue().toString());
		}
	}
  
/*
  public static String getServerTempPath() {
    return serverTempPath;
  }
*/
}
