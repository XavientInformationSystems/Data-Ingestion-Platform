package com.xavient.dip.apex.operator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.contrib.hbase.AbstractHBaseWindowPutOutputOperator;
import com.xavient.dip.common.config.DiPConfiguration;

public class HBaseSinkOperator extends AbstractHBaseWindowPutOutputOperator<Object[]> {

	@SuppressWarnings("unused")
	private String rowKey;
	private String columnFamily;
	private String[] columnFields;

	public HBaseSinkOperator(Configuration config) {
		this.rowKey = config.get(DiPConfiguration.HBASE_ROW_KEY);
		this.columnFamily = config.get(DiPConfiguration.HBASE_COL_FAMILY);
		this.columnFields = config.get(DiPConfiguration.HBASE_COL_NAMES).split(DiPConfiguration.HBASE_COL_DELIMITER);
	}

	@Override
	public Put operationPut(Object[] tuple) throws IOException {
		Put put = new Put(Bytes.toBytes(String.valueOf(tuple[1])));
		for (int i = 2; i < tuple.length; i++) {
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnFields[i - 2]),
					Bytes.toBytes(String.valueOf(tuple[i])));
		}
		return put;
	}

}
