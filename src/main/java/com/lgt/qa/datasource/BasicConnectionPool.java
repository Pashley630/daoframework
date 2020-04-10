package com.lgt.qa.datasource;

import org.apache.commons.dbcp2.BasicDataSourceFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 关系型数据库连接池
 *
 */
public class BasicConnectionPool {
	private DataSource ds;
	/**
	 * 根据配置文件创建关系型数据库连接池
	 * @param propertiesFilename
	 */
	public BasicConnectionPool(String propertiesFilename) {
		try {
			Properties prop = PropertiesUtils.read(propertiesFilename);
			ds = BasicDataSourceFactory.createDataSource(prop);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Connection getConnection() throws SQLException {
		if(ds != null) {
			return ds.getConnection();
		}else {
			return null;
		}
	}
	
	public DataSource getDataSource() {
		return ds;
	}
	
}
