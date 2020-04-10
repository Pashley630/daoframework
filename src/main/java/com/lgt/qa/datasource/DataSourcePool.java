package com.lgt.qa.datasource;

import javax.sql.DataSource;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 数据库连接池集合类，用于存放各种类型的连接池
 *
 */
public class DataSourcePool {
	private static Map<String, DataSource> RDBMS_DS_MAP = new HashMap<>(); // 关系型数据库连接池集合
	static {
		try {
			Properties prop = PropertiesUtils.read("db");
			if(prop != null) {
				Enumeration<?> names = prop.propertyNames();
				while(names.hasMoreElements()) {
					String name = names.nextElement().toString();
					if(name.startsWith("mysql")) {
						String propertiesFilename = prop.getProperty(name);
						if(propertiesFilename == null) {
							continue;
						}
						BasicConnectionPool bcp = new BasicConnectionPool(propertiesFilename);
						RDBMS_DS_MAP.put(name,bcp.getDataSource());
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 根据数据库连接池的名字获取关系型数据库连接池对象
	 * @param name 连接池名字
	 * @return 连接池对象
	 */
	public static DataSource getDataSource(String name) {
		return RDBMS_DS_MAP.get(name);
	}
}
