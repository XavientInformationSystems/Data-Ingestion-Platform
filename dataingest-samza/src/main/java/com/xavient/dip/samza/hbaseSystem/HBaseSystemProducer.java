package com.xavient.dip.samza.hbaseSystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.samza.config.Config;
import org.apache.samza.config.ScalaMapConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;

import com.xavient.dip.samza.Utils.ApplicationProperties;
import com.xavient.dip.samza.Utils.Constants;

import org.apache.log4j.Logger;

public class HBaseSystemProducer extends ScalaMapConfig implements SystemProducer, Serializable {

	private static final Logger log = Logger.getLogger(HBaseSystemProducer.class.getName());

	private static final long serialVersionUID = 149668434355161914L;
	Config conf;
	ApplicationProperties properties;

	public HBaseSystemProducer(Config config) throws IOException {
		super(config);
		properties = new ApplicationProperties(config);
		conf = config;
	}

	Configuration config;
	Admin hAdmin;
	Table table;
	Connection conn;
	HTableDescriptor hTableDesc;
	InputStream input;

	public void send(String source, OutgoingMessageEnvelope envelope) {

		try {
			String[] data = new String((byte[]) envelope.getMessage()).split(",");

			String[] columns = properties.getProp().getProperty(Constants.HBASE_COL_NAMES).split(properties.getProp().getProperty(Constants.HBASE_COL_DELIMITER));
			table = conn.getTable(TableName.valueOf( properties.getProp().getProperty(Constants.HBASE_TABLE_NAME)));
			Put p = new Put(Bytes.toBytes(String.valueOf(System.currentTimeMillis())));
			int index = 0;
			for (String s : columns) {
				p.addColumn(Bytes.toBytes( properties.getProp().getProperty(Constants.HBASE_TABLE_FAMILIES)), Bytes.toBytes(s),
						Bytes.toBytes((String) data[index]));
				index++;
			}

			table.put(p);
		} catch (IOException e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}

	}

	public void start() {

		config = HBaseConfiguration.create();
		config.set("hbase.master", properties.getProp().getProperty(Constants.HBASE_MASTER));
		config.set("hbase.zookeeper.quorum",  properties.getProp().getProperty(Constants.HBASE_ZOOKEEPER_QUORUM));
		config.set("hbase.zookeeper.property.clientPort", properties.getProp().getProperty(Constants.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT));
		config.set("zookeeper.znode.parent",  properties.getProp().getProperty(Constants.ZOOKEEPER_ZNODE_PARENT));
		
		
		try {

			conn = ConnectionFactory.createConnection(config);
			hAdmin = conn.getAdmin();
			hTableDesc = new HTableDescriptor(TableName.valueOf(properties.getProp().getProperty(Constants.HBASE_TABLE_NAME)));
			hTableDesc.addFamily(new HColumnDescriptor(properties.getProp().getProperty(Constants.HBASE_TABLE_FAMILIES)));
			if (!hAdmin.tableExists(hTableDesc.getTableName())) {
				log.info("Creating HBase Table : " + hTableDesc.getNameAsString());
				hAdmin.createTable(hTableDesc);
			}

		} catch (IOException e) {
			log.error("Error in Hbase Initialization :" + e.getMessage());
			e.printStackTrace();
		}

	}

	public void stop() {
		try {

			log.debug("Closing HBase Connection");
			hAdmin.close();
			conn.close();
		} catch (IOException e) {
			log.error("Error closing HBase Connections " + e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
	public void register(String source) {

	}

	@Override
	public void flush(String source) {

	}

}
