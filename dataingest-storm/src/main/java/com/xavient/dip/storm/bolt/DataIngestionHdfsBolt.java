package com.xavient.dip.storm.bolt;

import org.apache.storm.hdfs.bolt.HdfsBolt;

import com.xavient.dip.storm.utils.TupleHelpers;

import backtype.storm.tuple.Tuple;

public class DataIngestionHdfsBolt extends HdfsBolt {

	private static final long serialVersionUID = 3387283316207416282L;

	@Override
	public void execute(Tuple tuple) {
		if (!TupleHelpers.isTickTuple(tuple)) {
			super.execute(tuple);
		} else {
			this.collector.ack(tuple);
		}
	}

}
