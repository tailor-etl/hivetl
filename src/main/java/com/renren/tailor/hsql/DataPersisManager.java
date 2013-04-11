package com.renren.tailor.hsql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.h2.jdbcx.JdbcConnectionPool;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.renren.tailor.model.JobInfo;
import com.renren.tailor.util.JaskSonUtil;

public class DataPersisManager {
	
	private static Log logger = LogFactory.getLog(DataPersisManager.class);

	private static  JdbcConnectionPool cp;
	
	private static final String CREATE_SQL="create table if not exists tailor_job (id int auto_increment primary key,tableName varchar(128)," +
			"partitions varchar(255),status int,startTime varchar(32),endTime varchar(32),note varchar(512))";
	
	private static final String CHECK_JOB_RUNNED=" select count(1) from tailor_job where tableName=? and partitions=? and status=?";
	
	private static final String SAVE_SQL="insert into tailor_job (tableName,partitions,status,startTime,endTime,note) values(?,?,?,?,?,?)";
	
	
	static{
		try {
			Class.forName("org.h2.Driver");
			cp = JdbcConnectionPool.create("jdbc:h2:db/tailor.db","tailor","tailor");
			Connection con=cp.getConnection();
			Statement sta=con.createStatement();
			sta.execute(CREATE_SQL);
			//sta.execute("insert into tailor_job values(2,'lxb1','par=1234',1 )");
			sta.close();
			con.close();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}
	
	
	public static boolean saveJobInfo(JobInfo info){
		Connection conn=null;
		PreparedStatement pstmt=null;
		boolean flag=false;
		try {
			 conn=cp.getConnection();
			 pstmt=conn.prepareStatement(SAVE_SQL);
			 pstmt.setString(1, info.getTableName());
			 pstmt.setString(2, JaskSonUtil.getObjectMapper().writeValueAsString(info.getPartitions()));
			 pstmt.setInt(3, info.getResult());
			 pstmt.setString(4, info.getStartTime());
			 pstmt.setString(5, info.getEndTime());
			 pstmt.setString(6, info.getNote());
			 flag = pstmt.executeUpdate()>0?true:false;
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("sql exception "+e.getMessage());
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}finally{
			try{
				conn.close();
				pstmt.close();
			}catch (Exception e) {
				logger.error("can't close resources"+e.getMessage());
			}
		}
		return flag;
	}
	
	
	public static boolean checkJobRunned(String tableName,String partitions,int status){
		Connection conn=null;
		PreparedStatement pstmt=null;
		ResultSet rs=null;
		try {
			 conn=cp.getConnection();
			 pstmt=conn.prepareStatement(CHECK_JOB_RUNNED);
			 pstmt.setString(1, tableName);
			 pstmt.setString(2, partitions);
			 pstmt.setInt(3, status);
			 rs=pstmt.executeQuery();
			 if(rs.next()){
				 return rs.getInt(1)>0?true:false;
			 }
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("sql exception "+e.getMessage());
		}finally{
			try{
				conn.close();
				pstmt.close();
				rs.close();
			}catch (Exception e) {
				logger.error("can't close resources"+e.getMessage());
			}
		}
		return false;
	}
	
	public static void main(String[] args) throws SQLException{
		//checkJobRunned("lxb","par=123",0);
//		ResultSet rs=cp.getConnection().createStatement().executeQuery("select * from tailor_job");
//		  int colCount=rs.getMetaData().getColumnCount();
//	        while(rs.next()){
//	        	for(int i=0;i<colCount;i++){
//	                System.out.println(rs.getString(i+1));
//	              }
//	        }
		//System.out.println(cp.getConnection().createStatement().executeQuery("select * from tailor_job"));
	}
}
