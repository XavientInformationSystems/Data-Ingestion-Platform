package com.xavient.dip.storm.bolt;

import java.util.Arrays;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xavient.dip.common.utils.FlatJsonConverter;
import com.xavient.dip.storm.utils.TupleHelpers;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TwitterRawJsonConvertorBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 5844968075509153406L;
	private static final Logger LOGGER = LoggerFactory.getLogger(TwitterRawJsonConvertorBolt.class);
	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		if (!TupleHelpers.isTickTuple(tuple)) {
		try {
			Object[] data=FlatJsonConverter.convertToValuesArray(tuple.getString(0));
			collector.emit(new Values(Arrays.copyOfRange(data, 1, data.length)));
		} catch (Exception e) {
			LOGGER.error("Exception occurred while converting raw json to status", e);
		} finally{
			collector.ack(tuple);
		}
		}else{
			collector.ack(tuple);
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "text", "source", "reTweeted", "username", "createdAt", "retweetCount",
				"userLocation", "inReplyToUserId", "inReplyToStatusId", "userScreenName", "userDescription",
				"userFriendsCount", "userStatusesCount", "userFollowersCount"));
	}

}
