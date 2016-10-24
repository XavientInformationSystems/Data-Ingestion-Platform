package com.xavient.dip.storm.bolt;

import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;

import com.xavient.dip.storm.utils.TupleHelpers;

import backtype.storm.tuple.Tuple;

public class DataIngestionHBaseBolt extends HBaseBolt {

	public DataIngestionHBaseBolt(String tableName, HBaseMapper mapper) {
		super(tableName, mapper);
	}

	private static final long serialVersionUID = -1083629596717485391L;

	@Override
	public void execute(Tuple tuple) {
		if (!TupleHelpers.isTickTuple(tuple)) {
			super.execute(tuple);
		} else {
			this.collector.ack(tuple);
		}

	}

}
