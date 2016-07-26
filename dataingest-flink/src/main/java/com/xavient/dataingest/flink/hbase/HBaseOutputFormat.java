/*
 * OutputFormats are used to write to HBASE as no connectors for flink and HBASE are 
 * available by Apache
 */

package com.xavient.dataingest.flink.hbase;

import java.io.IOException;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.xavient.dataingest.flink.DataPayload;
import com.xavient.dataingest.flink.constants.Constants;
import com.xavient.dataingest.flink.exception.DataIngestException;
import com.xavient.dataingest.flink.util.CmdLineParser;
import com.xavient.dataingest.flink.vo.AppArgs;

public class HBaseOutputFormat<T> implements OutputFormat<DataPayload> {

	private org.apache.hadoop.conf.Configuration config = null;
	private HTable table = null;
	private String taskNumber = null;
	CmdLineParser parser = null;
	AppArgs appArgs = null;
	private int rowNumber = 0;
	HBaseConfiguration conf = null;

	
	
	private static final long serialVersionUID = 1L;

	@Override
	public void configure(Configuration parameters) {
		
		try {
			parser = new CmdLineParser();
			
				this.appArgs= parser.validateArgs(null);
			
						
			
			config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", appArgs.getProperty(Constants.ZK_HOST)+":" + appArgs.getProperty(Constants.ZK_PORT));
			config.set("hbase.master", appArgs.getProperty(Constants.HBASE_MASTER));
			config.set("zookeeper.znode.parent", "/hbase-unsecure");

			HBaseAdmin admin = new HBaseAdmin(config);

			if (!admin.tableExists(appArgs.getProperty(Constants.FLINK_HBASE_TABLENAME))) {
				System.out.println("camehere");
				TableName tableName = TableName.valueOf(appArgs.getProperty(Constants.FLINK_HBASE_TABLENAME));
				HTableDescriptor htd = new HTableDescriptor(tableName);
				HColumnDescriptor hcd = new HColumnDescriptor(appArgs.getProperty(Constants.HBASE_COL_FAMILIES));
				htd.addFamily(hcd);
				admin.createTable(htd);
			}
		} catch (IOException | DataIngestException e) {

		
		}

	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		table = new HTable(config, appArgs.getProperty(Constants.FLINK_HBASE_TABLENAME));
		this.taskNumber = String.valueOf(taskNumber);
	}

	@Override
	public void close() throws IOException {
		table.flushCommits();
		table.close();
	}

	@Override
	public void writeRecord(DataPayload arg0) throws IOException {

		String payload = arg0.toString();
		String columnna =appArgs.getProperty(Constants.HBASE_COL_NAMES);
		String[] columnnames = columnna.split("\\|");
		String[] ls = payload.split("\n");
		int index = 0;

		for (String s : ls) {

			String[] individualitems = s.split("\\|");

			Put put = new Put(Bytes.toBytes(Long.toString(System.currentTimeMillis())));
			for (String cn : columnnames) {
				put.add(Bytes.toBytes(appArgs.getProperty(Constants.HBASE_COL_FAMILIES)), Bytes.toBytes(cn), Bytes.toBytes(individualitems[index]));

				index++;

			}
			table.put(put);
			index = 0;
		}

	}

}