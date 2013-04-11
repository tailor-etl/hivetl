package com.renren.tailor.util;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PropertiesUtil {

  private final static Log logger = LogFactory.getLog(PropertiesUtil.class.getName());
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
  
  
  
/*
  public static String getServerTempPath() {
    return serverTempPath;
  }
*/
}
