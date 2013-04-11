package com.renren.tailor.conf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.security.auth.login.AppConfigurationEntry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

/**
 * tailor Configuration
 * @author 
 *
 */



public class TailorConf extends Configuration{

  protected String tailorJar;
  protected Properties origProp;
  protected String auxJars;
  private static URL tailorSiteURL = null;
  private static URL confVarURL = null;
  private static final Log log4j = LogFactory.getLog(TailorConf.class);
  
  
  static {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = TailorConf.class.getClassLoader();
    }

    // Log a warning if tailor-default.xml is found on the classpath
    URL tailorDefaultURL = classLoader.getResource("tailor-default.xml");
    if (tailorDefaultURL != null) {
      log4j.warn("DEPRECATED: Ignoring tailor-default.xml found on the CLASSPATH at " +
          tailorDefaultURL.getPath());
    }

    // Look for tailor-site.xml on the CLASSPATH and log its location if found.
    tailorSiteURL = classLoader.getResource("tailor-site.xml");
    if (tailorSiteURL == null) {
      log4j.warn("tailor-site.xml not found on CLASSPATH");
    } else {
      log4j.debug("Using tailor-site.xml found on CLASSPATH at " + tailorSiteURL.getPath());
    }
  }
  
  
  public static final TailorConf.ConfVars[] metaVars = {
    
    TailorConf.ConfVars.TAILORTWILISTENHOST,
    TailorConf.ConfVars.TAILORTWILISTENPORT,
    TailorConf.ConfVars.TAILORTWILISTENWAR,
    TailorConf.ConfVars.TAILORHDFSHOST,
    TailorConf.ConfVars.TAILORHDFSPORT,
    TailorConf.ConfVars.TAILORHDFSTEMPPATH
    
  };
  
  
  
  public static enum ConfVars {
    // TWI
    TAILORTWILISTENHOST("tailor.twi.listen.host", "0.0.0.0"),
    TAILORTWILISTENPORT("tailor.twi.listen.port", "9999"),
    TAILORTWILISTENWAR("tailor.twi.war.file", System.getenv("HWI_WAR_FILE")),
     
    TAILORHDFSHOST("tailor.hdfs.host", "10.5.18.230"),
    TAILORHDFSPORT("tailor.hdfs.port", "50070"),
    TAILORHDFSTEMPPATH("tailor.hdfs.temppath","/temp/tailor/");
    
    
    public final String varname;
    public final String defaultVal;
    public final int defaultIntVal;
    public final long defaultLongVal;
    public final float defaultFloatVal;
    public final Class<?> valClass;
    public final boolean defaultBoolVal;
    
    
    ConfVars(String varname, String defaultVal) {
      this.varname = varname;
      this.valClass = String.class;
      this.defaultVal = defaultVal;
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, int defaultIntVal) {
      this.varname = varname;
      this.valClass = Integer.class;
      this.defaultVal = Integer.toString(defaultIntVal);
      this.defaultIntVal = defaultIntVal;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, long defaultLongVal) {
      this.varname = varname;
      this.valClass = Long.class;
      this.defaultVal = Long.toString(defaultLongVal);
      this.defaultIntVal = -1;
      this.defaultLongVal = defaultLongVal;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, float defaultFloatVal) {
      this.varname = varname;
      this.valClass = Float.class;
      this.defaultVal = Float.toString(defaultFloatVal);
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = defaultFloatVal;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, boolean defaultBoolVal) {
      this.varname = varname;
      this.valClass = Boolean.class;
      this.defaultVal = Boolean.toString(defaultBoolVal);
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = defaultBoolVal;
    }
    
    
    @Override
    public String toString() {
      return varname;
    }
  }
  
  
  public static int getIntVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Integer.class);
    return conf.getInt(var.varname, var.defaultIntVal);
  }

  public static void setIntVar(Configuration conf, ConfVars var, int val) {
    assert (var.valClass == Integer.class);
    conf.setInt(var.varname, val);
  }

  public int getIntVar(ConfVars var) {
    return getIntVar(this, var);
  }

  public void setIntVar(ConfVars var, int val) {
    setIntVar(this, var, val);
  }

  public static long getLongVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Long.class);
    return conf.getLong(var.varname, var.defaultLongVal);
  }

  public static long getLongVar(Configuration conf, ConfVars var, long defaultVal) {
    return conf.getLong(var.varname, defaultVal);
  }

  public static void setLongVar(Configuration conf, ConfVars var, long val) {
    assert (var.valClass == Long.class);
    conf.setLong(var.varname, val);
  }

  public long getLongVar(ConfVars var) {
    return getLongVar(this, var);
  }

  public void setLongVar(ConfVars var, long val) {
    setLongVar(this, var, val);
  }

  public static float getFloatVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Float.class);
    return conf.getFloat(var.varname, var.defaultFloatVal);
  }


  public static boolean getBoolVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Boolean.class);
    return conf.getBoolean(var.varname, var.defaultBoolVal);
  }

  public static boolean getBoolVar(Configuration conf, ConfVars var, boolean defaultVal) {
    return conf.getBoolean(var.varname, defaultVal);
  }

  public static void setBoolVar(Configuration conf, ConfVars var, boolean val) {
    assert (var.valClass == Boolean.class);
    conf.setBoolean(var.varname, val);
  }

  public boolean getBoolVar(ConfVars var) {
    return getBoolVar(this, var);
  }

  public void setBoolVar(ConfVars var, boolean val) {
    setBoolVar(this, var, val);
  }

  public static String getVar(Configuration conf, ConfVars var) {
    assert (var.valClass == String.class);
    return conf.get(var.varname, var.defaultVal);
  }

  public static String getVar(Configuration conf, ConfVars var, String defaultVal) {
    return conf.get(var.varname, defaultVal);
  }

  public static void setVar(Configuration conf, ConfVars var, String val) {
    assert (var.valClass == String.class);
    conf.set(var.varname, val);
  }

  public String getVar(ConfVars var) {
    return getVar(this, var);
  }

  public void setVar(ConfVars var, String val) {
    setVar(this, var, val);
  }

  public void logVars(PrintStream ps) {
    for (ConfVars one : ConfVars.values()) {
      ps.println(one.varname + "=" + ((get(one.varname) != null) ? get(one.varname) : ""));
    }
  }
  
  private static synchronized URL getConfVarURL() {
    if (confVarURL == null) {
      try {
        Configuration conf = new Configuration();
        File confVarFile = File.createTempFile("tailor-default-", ".xml");
        confVarFile.deleteOnExit();

        applyDefaultNonNullConfVars(conf);

        FileOutputStream fout = new FileOutputStream(confVarFile);
        conf.writeXml(fout);
        fout.close();
        confVarURL = confVarFile.toURI().toURL();
      } catch (Exception e) {
        // We're pretty screwed if we can't load the default conf vars
        throw new RuntimeException("Failed to initialize default tailor configuration variables!", e);
      }
    }
    return confVarURL;
  }
  
  /**
   * Copy constructor
   */
  public TailorConf(TailorConf other) {
    super(other);
    tailorJar = other.tailorJar;
    auxJars = other.auxJars;
    origProp = (Properties)other.origProp.clone();
  }

  
  public TailorConf() {
    super();
    initialize(this.getClass());
  }

  public TailorConf(Class<?> cls) {
    super();
    initialize(cls);
  }

  public TailorConf(Configuration other, Class<?> cls) {
    super(other);
    initialize(cls);
  }
  
  
  private void initialize(Class<?> cls) {
    tailorJar = (new JobConf(cls)).getJar();

    // preserve the original configuration
    origProp = getAllProperties();

    // Overlay the ConfVars. Note that this ignores ConfVars with null values
    addResource(getConfVarURL());

    // Overlay tailor-site.xml if it exists
    if (tailorSiteURL != null) {
      addResource(tailorSiteURL);
  }
  }
  
  /**
   * Overlays ConfVar properties with non-null values
   */
  private static void applyDefaultNonNullConfVars(Configuration conf) {
    for (ConfVars var : ConfVars.values()) {
      if (var.defaultVal == null) {
        // Don't override ConfVars with null values
        continue;
      }
      if (conf.get(var.varname) != null) {
        log4j.debug("Overriding Hadoop conf property " + var.varname + "='" + conf.get(var.varname)
                  + "' with tailor default value '" + var.defaultVal +"'");
      }
      conf.set(var.varname, var.defaultVal);
    }
  }
  
  public Properties getChangedProperties() {
    Properties ret = new Properties();
    Properties newProp = getAllProperties();

    for (Object one : newProp.keySet()) {
      String oneProp = (String) one;
      String oldValue = origProp.getProperty(oneProp);
      if (!StringUtils.equals(oldValue, newProp.getProperty(oneProp))) {
        ret.setProperty(oneProp, newProp.getProperty(oneProp));
      }
    }
    return (ret);
  }
  
  

  public Properties getAllProperties() {
    return getProperties(this);
  }
  
  private static Properties getProperties(Configuration conf) {
    Iterator<Map.Entry<String, String>> iter = conf.iterator();
    Properties p = new Properties();
    while (iter.hasNext()) {
      Map.Entry<String, String> e = iter.next();
      p.setProperty(e.getKey(), e.getValue());
    }
    return p;
  }
  

  
  
  public AppConfigurationEntry[] getAppConfigurationEntry(String arg0) {
    return null;
  }


  public String getTailorJar() {
    return tailorJar;
  }


  public void setTailorJar(String tailorJar) {
    this.tailorJar = tailorJar;
  }


  public Properties getOrigProp() {
    return origProp;
  }


  public void setOrigProp(Properties origProp) {
    this.origProp = origProp;
  }


  public String getAuxJars() {
    return auxJars;
  }


  public void setAuxJars(String auxJars) {
    this.auxJars = auxJars;
  }


  public static URL getTailorSiteURL() {
    return tailorSiteURL;
  }


  public static void setTailorSiteURL(URL tailorSiteURL) {
    TailorConf.tailorSiteURL = tailorSiteURL;
  }


  public static TailorConf.ConfVars[] getMetavars() {
    return metaVars;
  }


  public static void setConfVarURL(URL confVarURL) {
    TailorConf.confVarURL = confVarURL;
  }
  

  
}
