package com.xavient.dip.spark;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.xavient.dip.common.AppArgs;
import com.xavient.dip.common.config.DiPConfiguration;
import com.xavient.dip.common.exceptions.DataIngestException;
import com.xavient.dip.common.utils.CmdLineParser;
import com.xavient.dip.common.utils.FlatJsonConverter;

import com.xavient.dip.spark.twitter.TopNLocationByTweets;
import com.xavient.dip.spark.twitter.TopNUsersWithMaxFollowers;

import com.xavient.dip.spark.writer.SparkHBaseWriter;
import com.xavient.dip.spark.writer.SparkHdfsWriter;
import com.xavient.dip.spark.writer.SparkJdbcSourceWriter;

public class TwitterDataIngestion {

	public static void main(String[] args) throws DataIngestException {
		CmdLineParser cmdLineParser = new CmdLineParser();
		final AppArgs appArgs = cmdLineParser.validateArgs(args);
		
		System.setProperty("HADOOP_USER_NAME", appArgs.getProperty(DiPConfiguration.HADOOP_USER_NAME));
		SparkConf conf = new SparkConf().setAppName("SparkTwitterStreaming")
				.setMaster("local[*]");
		try (JavaStreamingContext jsc = new JavaStreamingContext(new JavaSparkContext(conf), new Duration(1000))) {
			JavaPairReceiverInputDStream<String, String> stream = KafkaUtils.createStream(jsc,
					appArgs.getProperty(DiPConfiguration.ZK_HOST)+":"+appArgs.getProperty(DiPConfiguration.ZK_PORT), "spark-stream", getKafkaTopics(appArgs));
			JavaDStream<Object[]> twitterStreams = stream.map(tuple -> FlatJsonConverter.convertToValuesArray(tuple._2))
					.cache();
			SparkHdfsWriter.write(twitterStreams, appArgs);
			new SparkHBaseWriter(jsc.sparkContext(), appArgs).write(twitterStreams);
			SparkJdbcSourceWriter jdbcSourceWriter = new SparkJdbcSourceWriter(new SQLContext(jsc.sparkContext()),
					appArgs);
			new TopNLocationByTweets(jdbcSourceWriter,Integer.valueOf(appArgs.getProperty("topN"))).compute(twitterStreams);
			new TopNUsersWithMaxFollowers(jdbcSourceWriter,Integer.valueOf(appArgs.getProperty("topN"))).compute(twitterStreams);
			jsc.start();
			jsc.awaitTermination();
		}
	}

	private static Map<String, Integer> getKafkaTopics(AppArgs appArgs) {
		Map<String, Integer> topics = new HashMap<String, Integer>();
		topics.put(appArgs.getProperty(DiPConfiguration.KAFKA_TOPIC), 1);
		return topics;
	}

}
