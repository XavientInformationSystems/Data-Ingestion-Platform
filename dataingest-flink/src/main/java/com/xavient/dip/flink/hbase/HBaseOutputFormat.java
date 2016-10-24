package com.xavient.dip.flink.hbase;

import java.io.IOException;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.xavient.dip.common.AppArgs;
import com.xavient.dip.common.config.DiPConfiguration;

public class HBaseOutputFormat implements OutputFormat<Object[]> {

	private org.apache.hadoop.conf.Configuration config = null;
	private HTable table = null;
	AppArgs appArgs = null;
	HBaseConfiguration conf = null;
	private String columnFamily;
	private String[] columnFields;

	private static final long serialVersionUID = 1L;

	public HBaseOutputFormat(AppArgs appArgs) {
		this.appArgs = appArgs;
	}

	@Override
	public void configure(Configuration parameters) {
		try {
			this.columnFamily = appArgs.getProperty(DiPConfiguration.HBASE_COL_FAMILY);
			this.columnFields = appArgs.getProperty(DiPConfiguration.HBASE_COL_NAMES).split(DiPConfiguration.HBASE_COL_DELIMITER);
			config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum",
					appArgs.getProperty(DiPConfiguration.ZK_HOST) + ":" + appArgs.getProperty(DiPConfiguration.ZK_PORT));
			config.set("hbase.master", appArgs.getProperty(DiPConfiguration.HBASE_MASTER));
			config.set("zookeeper.znode.parent", "/hbase-unsecure");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		table = new HTable(config, appArgs.getProperty(DiPConfiguration.HBASE_TABLE_NAME));
	}

	@Override
	public void close() throws IOException {
		table.flushCommits();
		table.close();
	}

	@SuppressWarnings("deprecation")
	@Override
	public void writeRecord(Object[] record) throws IOException {
		Object[] data = (Object[]) record;
		Put put = new Put(Bytes.toBytes(String.valueOf(data[1])));
		for (int i = 2; i < data.length; i++) {
			put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columnFields[i - 2]),
					Bytes.toBytes(String.valueOf(data[i])));
		}
		table.put(put);
	}

}
