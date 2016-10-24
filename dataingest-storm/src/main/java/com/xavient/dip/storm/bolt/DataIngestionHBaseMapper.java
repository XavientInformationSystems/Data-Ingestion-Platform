package com.xavient.dip.storm.bolt;

import static org.apache.storm.hbase.common.Utils.toLong;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.common.ColumnList;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class DataIngestionHBaseMapper implements HBaseMapper {

	private static final long serialVersionUID = 7412080649211687669L;

	private String rowKeyField;
	private byte[] columnFamily;
	private Fields columnFields;
	private Fields counterFields;

	public DataIngestionHBaseMapper() {
	}

	public DataIngestionHBaseMapper withRowKeyField(String rowKeyField) {
		this.rowKeyField = rowKeyField;
		return this;
	}

	public DataIngestionHBaseMapper withColumnFields(Fields columnFields) {
		this.columnFields = columnFields;
		return this;
	}

	public DataIngestionHBaseMapper withCounterFields(Fields counterFields) {
		this.counterFields = counterFields;
		return this;
	}

	public DataIngestionHBaseMapper withColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily.getBytes();
		return this;
	}

	public byte[] rowKey(Tuple tuple) {
		Object objVal = tuple.getValueByField(this.rowKeyField);
		return toBytes(objVal);
	}

	public ColumnList columns(Tuple tuple) {
		ColumnList cols = new ColumnList();
		if (this.columnFields != null) {
			for (String field : this.columnFields) {
				cols.addColumn(this.columnFamily, field.getBytes(), toBytes(tuple.getValueByField(field)));
			}
		}
		if (this.counterFields != null) {
			for (String field : this.counterFields) {
				cols.addCounter(this.columnFamily, field.getBytes(), toLong(tuple.getValueByField(field)));
			}
		}
		return cols;
	}

	private byte[] toBytes(Object obj) {
		return Bytes.toBytes(String.valueOf(obj));
	}
}
