package com.xavient.dip.samza;

import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import com.xavient.dip.samza.Utils.FlatJsonConverter;
import com.xavient.dip.samza.hbaseSystem.HBaseSystemProducer;

public class ProcessTwitterFeed implements StreamTask, InitableTask {

	private static final Logger log = Logger.getLogger(HBaseSystemProducer.class.getName());
	Config conf;

	public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
			throws Exception {
		try {
			Object[] feed = FlatJsonConverter.convertToValuesArray((String) envelope.getMessage());
			Stream<Object> stream = Arrays.stream(feed);

			StringBuilder data = new StringBuilder();

			stream.forEach(x -> data.append("," + x));
			data.delete(0, 1);

			collector.send(new OutgoingMessageEnvelope(new SystemStream("hdfsDump", "hdfs"), data.toString()));
			collector.send(new OutgoingMessageEnvelope(new SystemStream("hdfsDump", "hdfs"), ""));
			collector.send(new OutgoingMessageEnvelope(new SystemStream("hbaseDump", "hbase"), data.toString()));
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}
	}

	@Override
	public void init(Config conf, TaskContext taskContext) throws Exception {

		this.conf = conf;

	}
}