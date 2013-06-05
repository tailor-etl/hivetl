package com.renren.tailor.hive;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;


public class ConnectionManager {

	private static DBConnectionPool connectionPool=null;
	
	public static void initialize(Configuration conf) throws Exception{
		connectionPool=new DBConnectionPool(conf.get("jdbc.driver"),conf.get("jdbc.url"),
				conf.get("jdbc.username"),conf.get("jdbc.password"));
		
		connectionPool.setInitialConnections(conf.getInt("jdbc.connection.pool.initial.connection",10));
		connectionPool.setIncrementalConnections(conf.getInt("jdbc.connection.pool.incremental.connection",10));
		connectionPool.setMaxConnections(conf.getInt("jdbc.connection.pool.max.connection",20));
		
		try{
			connectionPool.createPool();
		}catch(Exception e){
			throw new Exception(e.getMessage());
		}
	}
	/**
	 * 获取数据库连接
	 * @return
	 * @throws SQLException 
	 */
	public static Connection getConnection() throws SQLException {
		if(connectionPool!=null){
			return connectionPool.getConnection();
		}
		return null;
	}
	/**
	 * 向数据库连接池中归还连接，即释放连接
	 * @param conn
	 */
	public static void releaseConnection(Connection conn){
		if(connectionPool!=null){
			connectionPool.releaseConnection(conn);	
		}
	}
}
