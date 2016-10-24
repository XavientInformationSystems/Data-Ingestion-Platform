package com.xavient.dip.spark.writer;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.xavient.dip.common.AppArgs;
import com.xavient.dip.common.config.DiPConfiguration;

public class SparkHBaseWriter implements Serializable {

	private static final long serialVersionUID = -4652795987962410281L;
	private String tableName;
	private String columnFamily;
	private String[] columnFields;
	private JavaHBaseContext hbaseContext;

	public SparkHBaseWriter(JavaSparkContext jsc, AppArgs appArgs) {
		super();
		this.tableName = appArgs.getProperty(DiPConfiguration.HBASE_TABLE_NAME);
		this.columnFamily = appArgs.getProperty(DiPConfiguration.HBASE_COL_FAMILY);
		this.columnFields = appArgs.getProperty(DiPConfiguration.HBASE_COL_NAMES).split("\\|");
		this.hbaseContext = new JavaHBaseContext(jsc, getConf(appArgs));
	}

	public <T> void write(JavaDStream<T> stream) {
		hbaseContext.streamBulkPut(stream, TableName.valueOf(tableName), record -> {
			Object[] data = (Object[]) record;
			Put put = new Put(Bytes.toBytes(String.valueOf(data[1])));
			for (int i = 2; i < data.length; i++) {
				put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnFields[i - 2]),
						Bytes.toBytes(String.valueOf(data[i])));
			}
			return put;
		});
	}

	private static Configuration getConf(AppArgs appArgs) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", appArgs.getProperty(DiPConfiguration.HBASE_MASTER));
		conf.set("timeout", "120000");
		conf.set("hbase.zookeeper.quorum",
				appArgs.getProperty(DiPConfiguration.ZK_HOST) + ":" + appArgs.getProperty(DiPConfiguration.ZK_PORT));
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		return conf;
	}
}
