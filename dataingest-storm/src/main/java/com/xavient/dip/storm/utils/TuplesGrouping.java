package com.xavient.dip.storm.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.tuple.Tuple;



public class TuplesGrouping {

	public static Map<String, Integer> groupByField(List<Tuple> tuples, String field) {
		Map<String, Integer> counts = new HashMap<>();
		for (Tuple tuple : tuples) {
			if (counts.containsKey(tuple.getStringByField(field))) {
				counts.put(tuple.getStringByField(field), counts.get(tuple.getStringByField(field)) + 1);
			} else {
				if (tuple.getStringByField(field) != null)
					counts.put(tuple.getStringByField(field), 1);
			}
		}
		return counts;
	}

}
