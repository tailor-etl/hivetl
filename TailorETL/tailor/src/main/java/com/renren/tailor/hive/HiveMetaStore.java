package com.renren.tailor.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.jdbc.HiveConnection;
import org.apache.hadoop.hive.jdbc.HiveStatement;

import com.renren.tailor.model.RuleEngine;
import com.renren.tailor.util.ParameterUtil;
import com.renren.tailor.util.PropertiesUtil;

public class HiveMetaStore {

	private static Log logger = LogFactory.getLog(HiveMetaStore.class);
	private static final String HIVE_DRIVER = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static String HIVE_JDBC;

	static {
		try {
			Class.forName(HIVE_DRIVER);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
		HIVE_JDBC = "jdbc:hive://"
				+ PropertiesUtil.conf.get("hiveserver.ip", "10.4.24.90") + ":"
				+ PropertiesUtil.conf.get("hiveserver.port", "10000");
		logger.info("hiveip"+PropertiesUtil.conf.get("hiveserver.ip"));
	}

	private static final String SDID_SQL = "select max(SD_ID) from SDS where LOCATION like '?%'";

	/**
	 * 更新分区
	 * 
	 * @param engine
	 * @return
	 */
	public static int alterTable(RuleEngine engine) {
		int result = 0;
		Iterator<Entry<String, String>> it = engine.getPartitions().entrySet()
				.iterator();
		String sql = "alter table " + engine.getTableName()
				+ " add partition (";
		List<String> list = new ArrayList<String>();
		while (it.hasNext()) {
			Entry<String, String> en = it.next();
			list.add(en.getKey() + "=" + en.getValue());
			sql += en.getKey() + "='" + en.getValue() + "',";
		}
		sql = sql.substring(0, sql.length() - 1);
		String location = "";
		if (engine.getFieldRule() != null && engine.getFieldRule().size() > 0) {
			for (String s : list) {
				location += s + "/";
			}
			location = location.substring(0, location.length() - 1);
			sql += ") location '" + engine.getOutputPath() + location + "'";
		}else{
			sql+=") location '" + engine.getOutputPath().substring(0, engine.getOutputPath().lastIndexOf("/")) + "'";
		}
		try {
			alterPartition(sql, engine.getDbName());
			if (engine.getFieldRule() == null|| engine.getFieldRule().size() == 0) {
				alterMetastore(engine);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error(e.getMessage() + " alter table error " + sql);
			result = ParameterUtil.ErrorCode.ALTER_TABLE_ERROR;
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info(sql);
		return result;
	}

	public static void alterPartition(String sql, String dbName)
			throws SQLException {
		HiveConnection con = null;
		HiveStatement hs = null;
		try {
			con = (HiveConnection) DriverManager.getConnection(HIVE_JDBC, "",
					"");
			logger.info("hive connection:"+con);
			hs = (HiveStatement) con.createStatement();
			try {
				hs.executeUpdate("use " + dbName);
				logger.info("hive use db:"+dbName);
			} catch (Exception e) {
			}
			hs.executeUpdate(sql);
			logger.info("hive update:"+sql);
		} catch (SQLException e) {
			if (!e.getMessage().equals("Method not supported")) {
				e.printStackTrace();
				throw new SQLException(e.getMessage(), "hive update jdbc error");
			}
		} finally {
			try {
				con.close();
				hs.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static List<String> getPatchPartitions(RuleEngine ru)
			throws SQLException {
		HiveConnection con = null;
		HiveStatement hs = null;
		ResultSet rs = null;
		List<String> list = new LinkedList<String>();
		try {
			con = (HiveConnection) DriverManager.getConnection(HIVE_JDBC, "",
					"");
			hs = (HiveStatement) con.createStatement();
			try {
				hs.executeUpdate("use " + ru.getDbName());
			} catch (Exception e) {
			}
			rs = hs.executeQuery("show partitions " + ru.getTableName());
			while (rs.next()) {
				list.add(rs.getString(1));
			}
		} catch (SQLException e) {
			if (!e.getMessage().equals("Method not supported")) {
				e.printStackTrace();
				throw new SQLException(e.getMessage(), "hive update jdbc error");
			}
		} finally {
			try {
				con.close();
				hs.close();
				rs.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return list;
	}

	// 更新元数据
	public static void alterMetastore(RuleEngine engine) throws Exception {
		String path = engine.getOutputPath();
		path = path.substring(0, path.lastIndexOf("/"));
		logger.info("alter table path:"+path);
		Connection conn = null;
		Statement state = null;
		ResultSet rs = null;
		String sql = SDID_SQL.replaceFirst("\\?", engine.getHdfs() + path);
		try {
			conn = ConnectionManager.getConnection();
			conn.setAutoCommit(false);
			state = conn.createStatement();
			rs = state.executeQuery(sql);
			int sdid = -1;
			while (rs.next()) {
				sdid = rs.getInt(1);
				break;
			}
			logger.info("sdid:"+sdid+"  engine.getOutputPath()");
			state.executeUpdate("update  SDS set LOCATION='" + engine.getHdfs()
					+ engine.getOutputPath() + "' where SD_ID=" + sdid);
			conn.commit();
			conn.close();
		} catch (Exception e) {
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
				}
			}
			if (state != null) {
				try {
					state.close();
				} catch (SQLException e) {
				}
			}
			ConnectionManager.releaseConnection(conn);
		}
	}
}
