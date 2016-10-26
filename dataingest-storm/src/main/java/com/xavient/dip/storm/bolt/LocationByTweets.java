package com.xavient.dip.storm.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.xavient.dip.storm.utils.MapOrdering;
import com.xavient.dip.storm.utils.TuplesGrouping;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LocationByTweets extends DataIngestionBolt {

	private static final long serialVersionUID = 3682459986620893949L;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("location", "count"));
	}

	@Override
	protected void finishBatch() {
		List<Tuple> tuples = new ArrayList<Tuple>();
		queue.drainTo(tuples);
		Map<String, Integer> counts = TuplesGrouping.groupByField(tuples, "userLocation");
		List<Map.Entry<String, Integer>> lists = new ArrayList<>(counts.entrySet());
		Collections.sort(lists, new MapOrdering());
		lists = lists.size() > rankMaxThreshold ? lists.subList(0, rankMaxThreshold) : lists;
		for (Map.Entry<String, Integer> entry : lists)
			collector.emit(new Values(entry.getKey(), entry.getValue()));

	}

}
