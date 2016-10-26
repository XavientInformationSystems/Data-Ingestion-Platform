/*
 *  Main class which submits the flink program to the cluster . 
 *  It also creates the sink and the source  
 */
package com.xavient.dip.flink;

import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.DateTimeBucketer;
import org.apache.flink.streaming.connectors.fs.RollingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;


import com.xavient.dip.common.AppArgs;
import com.xavient.dip.common.config.DiPConfiguration;
import com.xavient.dip.common.utils.CmdLineParser;
import com.xavient.dip.common.utils.FlatJsonConverter;
import com.xavient.dip.flink.hbase.HBaseOutputFormat;

/**
 * Main Class For FlinkStreaming
 *
 */
public class FlinkTweeterStreamProcessor {
	public static void main(String[] args) throws Exception {

		CmdLineParser parser = new CmdLineParser();
		AppArgs appArgs = parser.validateArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);
		Properties properties = new Properties();
		properties.setProperty(DiPConfiguration.KAFKA_BOOTSTRAP_SERVERS,
				appArgs.getProperty(DiPConfiguration.KAFKA_BOOTSTRAP_SERVERS));
		properties.setProperty("zookeeper.connect",
				appArgs.getProperty(DiPConfiguration.ZK_HOST) + ":" + appArgs.getProperty(DiPConfiguration.ZK_PORT));
		properties.setProperty("group.id", DiPConfiguration.KAFKA_GROUP_ID);
		// Creates a source from which flink program picks up the data
		DataStream<String> kafkaSourceStream = env
				.addSource(new FlinkKafkaConsumer08<String>(appArgs.getProperty(DiPConfiguration.KAFKA_TOPIC),
						new SimpleStringSchema(), properties))
				.name("KafkaSource");
		DataStream<Object[]> tweeterStream = kafkaSourceStream
				.map(record -> FlatJsonConverter.convertToValuesArray(record)).name("Map Data");
		DataStream<String> hdfsStream = tweeterStream.map(record -> {
			StringBuilder recordBuilder = new StringBuilder();
			for (Object e : Arrays.copyOfRange(record, 1, record.length)) {
				recordBuilder.append(e);
				recordBuilder.append(appArgs.getProperty(DiPConfiguration.HDFS_OUTPUT_DELIMITER));
			}
			return StringUtils.removeEnd(recordBuilder.toString(),
					appArgs.getProperty(DiPConfiguration.HDFS_OUTPUT_DELIMITER));
		});
		System.setProperty("HADOOP_USER_NAME", appArgs.getProperty(DiPConfiguration.HADOOP_USER_NAME));
		// HDFS Sink to write the data to the HDFS
		RollingSink<String> hdfsSink = new RollingSink<>(appArgs.getProperty(DiPConfiguration.HDFS_OUTPUT_PATH));
		hdfsSink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HH-mm-ss"));
		hdfsSink.setBatchSize(1024 * 1024 * 400);
		hdfsSink.setInProgressPrefix("flink");
		hdfsSink.setInProgressSuffix(".text");
		hdfsStream.addSink(hdfsSink).name("HDFS Sink");
		// FLink writes to the HBASE using the HBASE output format
		tweeterStream.writeUsingOutputFormat(new HBaseOutputFormat(appArgs)).name("HBASE Sink");
		env.execute();
	}
}
