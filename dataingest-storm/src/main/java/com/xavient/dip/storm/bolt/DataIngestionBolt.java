package com.xavient.dip.storm.bolt;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xavient.dip.storm.utils.TupleHelpers;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public abstract class DataIngestionBolt extends BaseRichBolt {

	protected static final long serialVersionUID = 258276902869086374L;
	protected static final Logger LOG = LoggerFactory.getLogger(LocationByTweets.class);
	protected int batchSize;
	protected int batchIntervalInSec;
	protected Integer rankMaxThreshold;
	protected OutputCollector collector;
	protected long lastBatchProcessTimeSeconds = 0;
	protected LinkedBlockingQueue<Tuple> queue = new LinkedBlockingQueue<Tuple>();

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.batchSize = Integer.valueOf((String) stormConf.getOrDefault("batchSize", "100"));
		this.batchIntervalInSec = Integer.valueOf((String) stormConf.getOrDefault("batchIntervalInSec", "10"));
		this.rankMaxThreshold = Integer.valueOf((String) stormConf.getOrDefault("rankMaxThreshold", "50"));
	}

	@Override
	public void execute(Tuple tuple) {
		if (TupleHelpers.isTickTuple(tuple)) {
			if ((System.currentTimeMillis() / 1000 - lastBatchProcessTimeSeconds) >= batchIntervalInSec) {
				LOG.info("Current queue size is {}.But received tick tuple so executing the batch", this.queue.size());
				finishBatch();
			} else {
				LOG.info(
						"Current queue size is {}. Received tick tuple but last batch was executed {} seconds back that is less than {} so ignoring the tick tuple",
						this.queue.size(), (System.currentTimeMillis() / 1000 - lastBatchProcessTimeSeconds),
						batchIntervalInSec);
			}
		} else {
			// Add the tuple to queue. But don't ack it yet.
			this.queue.add(tuple);
			int queueSize = this.queue.size();
			LOG.info("current queue size is " + queueSize);
			if (queueSize >= batchSize) {
				LOG.info("Current queue size is >= {} executing the batch", batchSize);
				finishBatch();
			}

		}

	}

	abstract protected void finishBatch();

}
