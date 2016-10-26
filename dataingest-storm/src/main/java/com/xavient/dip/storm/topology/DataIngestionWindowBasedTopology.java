package com.xavient.dip.storm.topology;

import java.util.HashMap;
import java.util.Map;

import com.xavient.dip.common.AppArgs;
import com.xavient.dip.common.config.DiPConfiguration;
import com.xavient.dip.common.exceptions.DataIngestException;
import com.xavient.dip.common.utils.CmdLineParser;
import com.xavient.dip.storm.bolt.LocationByTweets;
import com.xavient.dip.storm.bolt.MySQLDataWriterBolt;
import com.xavient.dip.storm.bolt.TopNLocationByTweets;
import com.xavient.dip.storm.bolt.TopNUsersWithMaxFollowers;
import com.xavient.dip.storm.bolt.TwitterRawJsonConvertorBolt;
import com.xavient.dip.storm.bolt.UsersWithMaxFollowers;
import com.xavient.dip.storm.builder.HBaseBoltBuilder;
import com.xavient.dip.storm.builder.HdfsBoltBuilder;
import com.xavient.dip.storm.spout.kafka.KafkaSpoutFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class DataIngestionWindowBasedTopology {
	
	public static final String KAFKA_SPOUT_ID = "kafka-spout";
	public static final String FILTER_BOLT_ID = "filter-bolt";
	public static final String HDFS_BOLT_ID = "hdfs-bolt";
	public static final String HBASE_BOLT_ID = "hbase-bolt";
	public static void main(String[] args) throws DataIngestException {

		CmdLineParser parser = new CmdLineParser();
		AppArgs appArgs = parser.validateArgs(args);

		Map<String, Object> hbaseConfig = new HashMap<>();
		for (final String name : appArgs.getProperties().stringPropertyNames())
			hbaseConfig.put(name, appArgs.getProperties().getProperty(name));
		System.setProperty("HADOOP_USER_NAME", appArgs.getProperty(DiPConfiguration.HADOOP_USER_NAME));
		Config stormConf = new Config();
		stormConf.put("hbaseConfig", hbaseConfig);
		stormConf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Integer.valueOf(appArgs.getProperty("batchIntervalInSec","10")));
		stormConf.put("rankMaxThreshold", appArgs.getProperty("rankMaxThreshold", "100"));
	
		Map<String, Object> dbProperties = new HashMap<>();
		for (final String name : appArgs.getProperties().stringPropertyNames())
			dbProperties.put(name, appArgs.getProperties().getProperty(name));
		stormConf.put("dbProperties", dbProperties);

		new LocalCluster().submitTopology("DataIngestion", stormConf, buildTopology(appArgs));

		/*try {
			StormSubmitter.submitTopology("DataIngestion", stormConf, buildTopology(appArgs));
		} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
			throw new DataIngestException(e.getMessage());
		}*/
	}

	private static StormTopology buildTopology(AppArgs appArgs) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(KAFKA_SPOUT_ID, KafkaSpoutFactory.getKafkaSpout(appArgs), 2);
		
		builder.setBolt(FILTER_BOLT_ID, new TwitterRawJsonConvertorBolt())
				.shuffleGrouping(KAFKA_SPOUT_ID);
		
		builder.setBolt(HDFS_BOLT_ID, HdfsBoltBuilder.build(appArgs))
				.shuffleGrouping(FILTER_BOLT_ID);
		builder.setBolt(HBASE_BOLT_ID,
				HBaseBoltBuilder.build(appArgs, "hbaseConfig"))
				.shuffleGrouping(FILTER_BOLT_ID);

		builder.setBolt("USERS_MAX_FOLLOWERS", new UsersWithMaxFollowers()).shuffleGrouping(FILTER_BOLT_ID);
		builder.setBolt("TOPN_USERS_MAX_FOLLOWERS", new TopNUsersWithMaxFollowers())
				.globalGrouping("USERS_MAX_FOLLOWERS");

		builder.setBolt("LOCATION_BY_TWEETS", new LocationByTweets()).shuffleGrouping(FILTER_BOLT_ID);
		builder.setBolt("TOPN_LOCATION_BY_TWEETS", new TopNLocationByTweets()).globalGrouping("LOCATION_BY_TWEETS");

		builder.setBolt("MYSQL_WRITER", new MySQLDataWriterBolt(),2)
				.fieldsGrouping("TOPN_USERS_MAX_FOLLOWERS", new Fields("tableName"))
				.fieldsGrouping("TOPN_LOCATION_BY_TWEETS", new Fields("tableName"));
		return builder.createTopology();
	}

}
