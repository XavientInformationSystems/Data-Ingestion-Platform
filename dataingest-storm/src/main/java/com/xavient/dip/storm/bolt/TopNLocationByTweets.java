package com.xavient.dip.storm.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.xavient.dip.storm.utils.MapOrdering;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TopNLocationByTweets extends DataIngestionBolt {

	private static final long serialVersionUID = 588968842105801987L;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.batchSize = rankMaxThreshold*3;
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tableName", "location", "count"));
	}

	@Override
	protected void finishBatch() {
		List<Tuple> tuples = new ArrayList<Tuple>();
		queue.drainTo(tuples);
		Map<String, Integer> counts = groupByField(tuples, "location");
		List<Map.Entry<String, Integer>> lists = new ArrayList<>(counts.entrySet());
		Collections.sort(lists, new MapOrdering());
		lists = lists.size() > rankMaxThreshold ? lists.subList(0, rankMaxThreshold) : lists;
		for (Map.Entry<String, Integer> entry : lists){
			collector.emit(new Values("tweets_location", entry.getKey(), entry.getValue()));
			}
	}

	private Map<String, Integer> groupByField(List<Tuple> tuples, String field) {
		Map<String, Integer> counts = new HashMap<>();
		for (Tuple tuple : tuples) {
			if (counts.containsKey(tuple.getStringByField(field))) {
				counts.put(tuple.getStringByField(field), counts.get(tuple.getStringByField(field)) + 1);
			} else {
				counts.put(tuple.getStringByField(field), tuple.getIntegerByField("count"));
			}
			collector.ack(tuple);
		}
		return counts;
	}

}
