package com.renren.tailor.hive;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.hive.jdbc.HiveConnection;
import org.apache.hadoop.hive.jdbc.HiveStatement;

public class HiveMetaStore {

	private static final String HIVE_DRIVER = "org.apache.hadoop.hive.jdbc.HiveDriver";

	static {
		try {
			Class.forName(HIVE_DRIVER);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static void alterPartition(String sql) throws SQLException {
		HiveConnection con = null;
		HiveStatement hs = null;
		try {
			con = (HiveConnection) DriverManager.getConnection(
					"jdbc:hive://10.9.16.42:10000/default", "", "");
			hs = (HiveStatement) con.createStatement();
			hs.executeUpdate(sql);
			System.out.println(hs);
		} catch (SQLException e) {
			 if (!e.getMessage().equals("Method not supported")) {
			        e.printStackTrace();
			        throw new SQLException(e.getMessage(), "hive update jdbc error");
			      }
		}finally{
			try {
				con.close();
				hs.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
