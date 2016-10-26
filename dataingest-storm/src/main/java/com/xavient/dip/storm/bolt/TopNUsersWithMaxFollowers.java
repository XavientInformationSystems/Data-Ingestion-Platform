package com.xavient.dip.storm.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.xavient.dip.storm.utils.TuplesOrdering;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TopNUsersWithMaxFollowers extends DataIngestionBolt {

	private static final long serialVersionUID = 588968842105801987L;

	@SuppressWarnings({ "rawtypes" })
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		this.batchSize = rankMaxThreshold * 3;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tableName", "username", "count"));
	}

	@Override
	protected void finishBatch() {
		List<Tuple> _tuples = new ArrayList<Tuple>();
		queue.drainTo(_tuples);
		Set<Tuple> tuples = new TreeSet<>(new TuplesOrdering("followersCount"));
		tuples.addAll(_tuples);
		tuples = tuples.size() > rankMaxThreshold ? ImmutableSet.copyOf(Iterables.limit(tuples, rankMaxThreshold))
				: tuples;
		for (Tuple tuple : tuples) {
			collector.emit(new Values("user_followers", tuple.getStringByField("username"),
					tuple.getIntegerByField("followersCount")));
			collector.ack(tuple);
		}
		
	}

}
