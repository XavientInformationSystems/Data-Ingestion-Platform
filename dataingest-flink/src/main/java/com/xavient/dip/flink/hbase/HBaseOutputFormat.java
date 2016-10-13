package com.xavient.dip.flink.hbase;

import java.io.IOException;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.xavient.dataingest.flink.constants.Constants;
import com.xavient.dataingest.flink.util.CmdLineParser;
import com.xavient.dataingest.flink.vo.AppArgs;

public class HBaseOutputFormat implements OutputFormat<Object[]> {

	private org.apache.hadoop.conf.Configuration config = null;
	private HTable table = null;
	CmdLineParser parser = null;
	AppArgs appArgs = null;
	HBaseConfiguration conf = null;
	private String columnFamily;
	private String[] columnFields;

	private static final long serialVersionUID = 1L;

	@Override
	public void configure(Configuration parameters) {
		try {
			parser = new CmdLineParser();
			this.appArgs = parser.validateArgs(null);
			this.columnFamily = appArgs.getProperty(Constants.HBASE_COL_FAMILIES);
			this.columnFields = appArgs.getProperty(Constants.HBASE_COL_NAMES).split("\\|");
			config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum",
					appArgs.getProperty(Constants.ZK_HOST) + ":" + appArgs.getProperty(Constants.ZK_PORT));
			config.set("hbase.master", appArgs.getProperty(Constants.HBASE_MASTER));
			config.set("zookeeper.znode.parent", "/hbase-unsecure");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		table = new HTable(config, appArgs.getProperty(Constants.FLINK_HBASE_TABLENAME));
	}

	@Override
	public void close() throws IOException {
		table.flushCommits();
		table.close();
	}

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
