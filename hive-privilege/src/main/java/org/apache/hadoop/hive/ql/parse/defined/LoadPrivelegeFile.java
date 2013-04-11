package org.apache.hadoop.hive.ql.parse.defined;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import com.sun.org.apache.xpath.internal.XPathAPI;

/**   
* @Title: LoadPrivelegeFile.java 
* @Package org.apache.hadoop.hive.ql.parse.defined 
* @Description: 加载权限配置文件 conf/privilege-file.xml
* @author xianbing.liu   
* @date 2013-2-16 上午11:45:54 
* @version V1.0   
*/ 

public class LoadPrivelegeFile {

	private static final Log l4j = LogFactory.getLog(LoadPrivelegeFile.class);

	private static ScheduledExecutorService scheduleExecutor = null;

	private static volatile CaseInsensitiveMap dataBaseTableMaps = null;

	private static volatile CaseInsensitiveMap userMaps = null;

	public static final String TOK_TABNAME = "TOK_TABNAME(";

	private static String TMP_TABLE_RULE = "temp_";

	private static boolean VALID_PRIVILEGE = true;

	private static int UPDATE_INTERVAL = -1;// 权限配置文件更新间隔

	private static long FILE_MODIFY_TIME = 0l;

	private static URL privilegeFileURL = null;

	static {
		ClassLoader classLoader = Thread.currentThread()
				.getContextClassLoader();
		if (classLoader == null) {
			classLoader = HivePrivilegeHook.class.getClassLoader();
		}
		privilegeFileURL = classLoader.getResource("privilege-file.xml");
		if (privilegeFileURL == null) {
			l4j.warn("privilege-file.xml not found on CLASSPATH");
		} else {
			loadPrivilegeFile();
			l4j.debug(dataBaseTableMaps);
			l4j.debug(userMaps);
			FILE_MODIFY_TIME = new File(privilegeFileURL.getPath())
					.lastModified();
		}
	}

	/**
	 * @Title: loadPrivilegeFile
	 * @Description: 加载权限配置文件
	 * @return void 返回类型
	 * @throws
	 */
	private static void loadPrivilegeFile() {
		CaseInsensitiveMap dataBaseTableMapsTmp = new CaseInsensitiveMap();
		CaseInsensitiveMap userMapsTmp = new CaseInsensitiveMap();
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setIgnoringComments(true);
		factory.setNamespaceAware(true);
		DocumentBuilder builder = null;
		Document document = null;
		InputStream is = null;
		String tableName = null;
		String dbName = null;
		Set<String> userList = null;
		Set<String> tableSets = null;
		NodeList groupList = null;

		try {
			builder = factory.newDocumentBuilder();
			is = privilegeFileURL.openStream();
			document = builder.parse(is);
			org.w3c.dom.Node singleNode = (org.w3c.dom.Node) XPathAPI
					.selectSingleNode(document, "/Privilege");
			String updateInterval = singleNode.getAttributes()
					.getNamedItem("update_interval").getNodeValue().trim();
			if (null != updateInterval && updateInterval.matches("[0-9]+")) {
				int interval = Integer.parseInt(updateInterval);
				if (interval > 0 && UPDATE_INTERVAL != interval) {
					UPDATE_INTERVAL = interval;
					shutDownThread();
					scheduleExecutor = Executors
							.newSingleThreadScheduledExecutor();
					scheduleExecutor.scheduleAtFixedRate(
							new FileUpdateThread(), 0, UPDATE_INTERVAL,
							TimeUnit.SECONDS);
				}
			}
			String tmptablename = singleNode.getAttributes()
					.getNamedItem("tmptablename").getNodeValue().trim();
			if (null != tmptablename && tmptablename.length() > 0) {
				TMP_TABLE_RULE = tmptablename;
			}
			String valid = singleNode.getAttributes().getNamedItem("valid")
					.getNodeValue().trim();
			VALID_PRIVILEGE = Boolean.parseBoolean(valid);

			if (null != valid && tmptablename.length() > 0) {
				TMP_TABLE_RULE = tmptablename;
			}
			NodeList dataBases = XPathAPI.selectNodeList(document,
					"/Privilege/Database");
			for (int i = 0; i < dataBases.getLength(); i++) {
				tableSets = new HashSet<String>();
				dbName = dataBases.item(i).getAttributes().getNamedItem("name")
						.getNodeValue().trim();
				NodeList tables = XPathAPI.selectNodeList(document,
						"/Privilege/Database[@name='" + dbName + "']/Table");
				for (int p = 0; p < tables.getLength(); p++) {
					tableName = tables.item(p).getAttributes()
							.getNamedItem("name").getNodeValue().trim();
					tableSets.add(TOK_TABNAME + tableName + ")");
					tableSets
							.add(TOK_TABNAME + dbName + ")(" + tableName + ")");
					NodeList nodeList = XPathAPI.selectNodeList(document,
							"/Privilege/Database[@name='" + dbName
									+ "']/Table[@name='" + tableName + "']/*");
					for (int j = 0; j < nodeList.getLength(); j++) {
						String nodeName = nodeList.item(j).getNodeName();
						NodeList nodes = XPathAPI.selectNodeList(document,
								"/Privilege/Database/Table[@name='" + tableName
										+ "']/" + nodeName + "/user");
						userList = new HashSet<String>();
						for (int k = 0; k < nodes.getLength(); k++) {
							userList.add(nodes.item(k).getTextContent());
						}
						nodes = XPathAPI.selectNodeList(document,
								"/Privilege/Database/Table[@name='" + tableName
										+ "']/" + nodeName + "/group");
						for (int k = 0; k < nodes.getLength(); k++) {
							String groupname = nodes.item(k).getTextContent();
							groupList = XPathAPI.selectNodeList(document,
									"/Privilege/Groups/Group[@name='"
											+ groupname + "']/user");
							for (int w = 0; w < groupList.getLength(); w++) {
								userList.add(groupList.item(w).getTextContent());
							}
						}
						userMapsTmp.put(dbName + TOK_TABNAME + tableName + ")"
								+ nodeName, userList);
						userMapsTmp.put(dbName + TOK_TABNAME + dbName + ")("
								+ tableName + ")" + nodeName, userList);
					}
				}
				userList = new HashSet<String>();
				tables = XPathAPI.selectNodeList(document,
						"/Privilege/Database[@name='" + dbName
								+ "']/Create/user");
				for (int p = 0; p < tables.getLength(); p++) {
					userList.add(tables.item(p).getTextContent());
				}
				tables = XPathAPI.selectNodeList(document,
						"/Privilege/Database[@name='" + dbName
								+ "']/Create/group");
				for (int k = 0; k < tables.getLength(); k++) {
					String groupname = tables.item(k).getTextContent();
					groupList = XPathAPI.selectNodeList(document,
							"/Privilege/Groups/Group[@name='" + groupname
									+ "']/user");
					for (int w = 0; w < groupList.getLength(); w++) {
						userList.add(groupList.item(w).getTextContent());
					}
				}
				userMapsTmp.put(dbName + "Create", userList);

				dataBaseTableMapsTmp.put(dbName, tableSets);
			}
		} catch (Exception e) {
			e.printStackTrace();
			l4j.warn("privilege-file.xml parse error");
		}

		finally {
			if (null != is) {
				try {
					is.close();
				} catch (IOException e) {
				}
			}
			dataBaseTableMaps = dataBaseTableMapsTmp;
			userMaps = userMapsTmp;
		}
	}

	public static CaseInsensitiveMap getDatabasetablemaps() {
		return dataBaseTableMaps;
	}

	public static CaseInsensitiveMap getUsermaps() {
		return userMaps;
	}

	public static boolean getIfValid() {
		return VALID_PRIVILEGE;
	}

	public static String getTmpTableRule() {
		return TMP_TABLE_RULE;
	}

	// public static long updateMaps(){
	// CaseInsensitiveMap dataBaseTableMapsTmp = new CaseInsensitiveMap();
	// dataBaseTableMaps=dataBaseTableMapsTmp;
	// return System.currentTimeMillis();
	// }

	private static class FileUpdateThread implements Runnable {
		@Override
		public void run() {
			try {
				File f = new File(privilegeFileURL.getPath());
				if (f.lastModified() > FILE_MODIFY_TIME) {
					FILE_MODIFY_TIME = f.lastModified();
					l4j.warn("file modified...");
					loadPrivilegeFile();
				}
			} catch (Throwable e) {
				e.printStackTrace();
			}

		}
	}

	private static void shutDownThread() throws InterruptedException {
		if (scheduleExecutor != null) {
			scheduleExecutor.shutdown();
			if (!scheduleExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
				scheduleExecutor.shutdownNow();
			}
		}
	}
}
