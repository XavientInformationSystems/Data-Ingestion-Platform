package com.xavient.dip.storm.utils;

import java.util.Comparator;

import backtype.storm.tuple.Tuple;



public class TuplesOrdering implements Comparator<Tuple> {

	private String field;

	public TuplesOrdering(String field) {
		super();
		this.field = field;
	}

	@Override
	public int compare(Tuple tuple1, Tuple tuple2) {
		return Integer.compare(tuple2.getIntegerByField(field), tuple1.getIntegerByField(field));
	}

}
