package com.xavient.dip.storm.utils;

import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSourceFactory;

public class DataSourceFactory {


	public static DataSource getDataSource(Map<String, String> map) {
		try {
			Properties properties=new Properties();
			for(String key:map.keySet())
				properties.put(key, map.get(key));
			return BasicDataSourceFactory.createDataSource(properties);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
