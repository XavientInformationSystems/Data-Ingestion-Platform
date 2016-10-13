package com.xavient.dataingest.storm.bolt.hbase;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class HBaseDumpBolt extends BaseRichBolt {

	private static final long serialVersionUID = -4275676626356010756L;
	final static Logger logger = LoggerFactory.getLogger(HBaseDumpBolt.class);

	private OutputCollector _collector;
	private static transient HBaseConnector connector = null;
	private static transient HBaseConfiguration conf = null;
	private static transient HBaseCommunicator communicator = null;

	private List<String> colFamilyNames;
	private List<List<String>> colNames;
	private List<List<String>> colValues;
	private List<String> colFamilyValues;
	private String hbaseXmlLocation;

	private Date today;
	private long time;
	private Timestamp timestamp;

	private String rowKeyCheck = null, rowKey = null, fieldValue = null, tableName = null;
	private static int counter = 0;

	public HBaseDumpBolt(final String hbaseXmlLocation, final String tableName, final String rowKeyCheck,
			final List<String> colFamilyNames, final List<List<String>> colNames) {

		logger.info("HBASE PROP FILE --> " + hbaseXmlLocation);

		this.tableName = tableName;
		this.colFamilyNames = colFamilyNames;
		this.colNames = colNames;
		this.rowKeyCheck = rowKeyCheck;
		this.hbaseXmlLocation = hbaseXmlLocation;

	}

	@Override
	public void execute(Tuple tuple) {

		logger.info("Tuple size: " + tuple.size() + ". to String: " + tuple.getFields().toString());

		counter = 0;
		rowKey = null;

		colValues = new ArrayList<List<String>>();
		colFamilyValues = new ArrayList<>();

		if (colFamilyNames.size() == 1) {
			for (int j = 0; j < colNames.get(0).size(); j++) {
				fieldValue = tuple.getValue(j).toString();
				if (rowKeyCheck.equals(colNames.get(0).get(j))) {
					rowKey = fieldValue;
				}
				colFamilyValues.add(fieldValue);
			}
			colValues.add(colFamilyValues);
		} else {
			for (int i = 0; i < colFamilyNames.size(); i++) {
				for (int j = 0; j < colNames.get(i).size(); j++) {
					fieldValue = tuple.getValue(counter).toString();
					if (rowKeyCheck.equals(colNames.get(i).get(j))) {
						rowKey = fieldValue;
					}
					colFamilyValues.add(fieldValue);
					counter++;
				}
				colValues.add(colFamilyValues);
				colFamilyValues = new ArrayList<String>();
			}
		}
		if (rowKeyCheck.equals("timestamp") && rowKey == null) {
			today = new Date();
			timestamp = new Timestamp(today.getTime());
			time = timestamp.getTime();
			rowKey = String.valueOf(time);
		}

		/*
		 * logger.info("HBASE COMM: " + communicator.tableExists("books"));
		 * logger.info("{} {} {} {} {}", rowKey, tableName, colFamilyNames,
		 * colNames, colValues);
		 */
		communicator.addRow(rowKey, tableName, colFamilyNames, colNames, colValues);
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map arg0, TopologyContext arg1, OutputCollector collector) {
		this._collector = collector;
		connector = new HBaseConnector();

		conf = connector.getHBaseConf(hbaseXmlLocation);
		logger.info("HBASE CONF: " + conf.toString());
		communicator = new HBaseCommunicator(conf);

		// check if tableName already exists
		if (colFamilyNames.size() == colNames.size()) {
			if (!communicator.tableExists(tableName)) {
				communicator.createTable(tableName, colFamilyNames);
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

}
