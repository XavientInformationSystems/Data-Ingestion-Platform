package com.xavient.dip.storm.utils;

import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.tuple.Tuple;

public class BatchHolder {

	/** The queue holding tuples in a batch. */

	protected LinkedBlockingQueue<Tuple> queue = new LinkedBlockingQueue<Tuple>();

	/** The threshold after which the batch should be flushed out. */

	int batchSize = 100;
	/**

	 * The batch interval in sec. Minimum time between flushes if the batch sizes

	 * are not met. This should typically be equal to

	 * topology.tick.tuple.freq.secs and half of topology.message.timeout.secs

	 */
	int batchIntervalInSec = 45;

	/** The last batch process time seconds. Used for tracking purpose */
	long lastBatchProcessTimeSeconds = 0;
}
