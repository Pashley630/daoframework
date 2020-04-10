package com.lgt.qa.dbutils;

import com.lgt.qa.datasource.DataSourcePool;

import javax.sql.RowSet;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 封装关系型数据库常用命令
 *
 */
public class ReleativeDBUtils {
	/**
	 * 执行select查询语句，仅支持返回单行，如果查询有多行返回，则值返回的map中只有第一行记录
	 * @param poolName 连接池名字
	 * @param sql 执行的sql语句，支持预编译的占位符语法
	 * @param args 传入的sql中的替换参数
	 * @return 查询的结果集，map中是字段名和值的键值对
	 * @throws Exception 
	 */
	public static Map<String, String> select(String poolName, String sql, List<Object> args) throws Exception{
		Map<String, String> resultMap = new HashMap<>();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = DataSourcePool.getDataSource(poolName).getConnection();
			ps = conn.prepareStatement(sql);
			// 将替换参数传入sql中
			for(int i = 0; i < args.size(); i++) {
				ps.setObject(i+1, args.get(i));
			}
			rs = ps.executeQuery();
			// 将字段名称（label）和值传入map集合
			int j=0;
			while(rs.next()) {
				ResultSetMetaData rsmd = rs.getMetaData();
				for(int i = 0; i < rsmd.getColumnCount(); i++) {
					String key="";
					if(j == 0) {
						key = rsmd.getColumnLabel(i+1);
					}else {
						key = rsmd.getColumnLabel(i+1)+j;
					}
//					String key = rsmd.getColumnLabel(i+1);
					String value = rs.getObject(i+1).toString();
					resultMap.put(key, value);
				}
				j++;
			}			
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			try {
				close(conn,ps,rs);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return resultMap;
	}
	
	/**
	 * 王健
	 * 执行select查询语句，支持返回多行
	 * @param poolName 连接池名字
	 * @param sql 执行的sql语句
	 * @return 查询的结果集
	 * @throws SQLException 
	 */
	public static RowSet select(String poolName, String sql) throws SQLException{
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;		
		RowSetFactory factory =null;  
        CachedRowSet crs = null;  
        
		try {			
			conn = DataSourcePool.getDataSource(poolName).getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			
			factory = RowSetProvider.newFactory();  
	        crs = factory.createCachedRowSet();  
	        crs.populate(rs);  
			
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			try {
				close(conn,ps,rs);				
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return crs;
	}
	
	/**
	 * 执行DML语句，包括insert，update，delete
	 * @param poolName 数据库连接池名称
	 * @param sql 执行的DML语句，支持预编译的占位符语法
	 * @param args 传入的sql中的替换参数
	 */
	public static void update(String poolName, String sql, List<Object> args) {
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = DataSourcePool.getDataSource(poolName).getConnection();
			ps = conn.prepareStatement(sql);
			for(int i = 0; i < args.size(); i++) {
				ps.setObject(i+1, args.get(i));
			}
			ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				close(conn,ps);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 关闭连接的封装方法，由于使用了连接池，所有connection的close方法并非真正关闭连接，而是将当前使用的connection对象返回到连接池中
	 * @param conn 连接对象
	 * @param stmt 语句对象
	 * @param rs 结果集对象
	 * @throws SQLException 关闭出现异常
	 */
	public static void close(Connection conn, Statement stmt, ResultSet rs) throws SQLException {
		if(rs != null) {
			rs.close();
		}
		if(stmt != null) {
			stmt.close();
		}
		if(conn != null) {
			conn.close();
		}
	}
	
	public static void close(Connection conn, Statement stmt) throws SQLException {
		close(conn,stmt,null);
	}
	
	public static void close(Connection conn) throws SQLException{
		close(conn,null,null);
	}
}
