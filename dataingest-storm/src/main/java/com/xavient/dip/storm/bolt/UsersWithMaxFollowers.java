package com.xavient.dip.storm.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.xavient.dip.storm.utils.TuplesOrdering;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class UsersWithMaxFollowers extends DataIngestionBolt {

	private static final long serialVersionUID = 3682459986620893949L;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("username", "followersCount"));
	}

	@Override
	protected void finishBatch() {
		List<Tuple> _tuples = new ArrayList<Tuple>();
		queue.drainTo(_tuples);
		Set<Tuple> tuples = new TreeSet<>(new TuplesOrdering("userFollowersCount"));
		tuples.addAll(_tuples);
		tuples = tuples.size() > rankMaxThreshold ? ImmutableSet.copyOf(Iterables.limit(tuples, rankMaxThreshold))
				: tuples;
		for (Tuple tuple : tuples)
			collector.emit(
					new Values(tuple.getStringByField("username"), tuple.getIntegerByField("userFollowersCount")));

	}

}
