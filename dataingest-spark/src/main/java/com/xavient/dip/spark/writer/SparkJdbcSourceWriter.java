package com.xavient.dip.spark.writer;

import java.util.List;
import java.util.Properties;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;

import com.xavient.dip.spark.util.AppArgs;

public class SparkJdbcSourceWriter {

	private Properties props;
	private SQLContext sqlContext;

	public SparkJdbcSourceWriter(SQLContext sqlContext, AppArgs appArgs) {
		super();
		this.sqlContext = sqlContext;
		this.props = new Properties();
		props.setProperty("url", appArgs.getProperty("jdbc.url"));
		props.setProperty("user", appArgs.getProperty("jdbc.user"));
		props.setProperty("password", appArgs.getProperty("jdbc.password"));
		props.setProperty("driver", appArgs.getProperty("jdbc.driver.class"));
	}

	public void write(List<Row> rows, StructType schema, String tableName) {
		if (CollectionUtils.isNotEmpty(rows))
			sqlContext.createDataFrame(rows, schema).write().mode(SaveMode.Overwrite).jdbc(props.getProperty("url"),
					tableName, props);
	}
}
