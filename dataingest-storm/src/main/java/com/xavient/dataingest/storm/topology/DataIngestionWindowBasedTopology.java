package com.xavient.dataingest.storm.topology;

import java.util.HashMap;
import java.util.Map;

import com.xavient.dataingest.storm.bolt.LocationByTweets;
import com.xavient.dataingest.storm.bolt.MySQLDataWriterBolt;
import com.xavient.dataingest.storm.bolt.TopNLocationByTweets;
import com.xavient.dataingest.storm.bolt.TopNUsersWithMaxFollowers;
import com.xavient.dataingest.storm.bolt.TwitterRawJsonConvertorBolt;
import com.xavient.dataingest.storm.bolt.UsersWithMaxFollowers;
import com.xavient.dataingest.storm.builder.HBaseBoltBuilder;
import com.xavient.dataingest.storm.builder.HdfsBoltBuilder;
import com.xavient.dataingest.storm.constants.Constants;
import com.xavient.dataingest.storm.exception.DataIngestException;
import com.xavient.dataingest.storm.spout.kafka.KafkaSpoutFactory;
import com.xavient.dataingest.storm.util.CmdLineParser;
import com.xavient.dataingest.storm.vo.AppArgs;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class DataIngestionWindowBasedTopology {

	public static void main(String[] args) throws DataIngestException {

		CmdLineParser parser = new CmdLineParser();
		AppArgs appArgs = parser.validateArgs(args);

		Map<String, Object> hbaseConfig = new HashMap<>();
		for (final String name : appArgs.getProperties().stringPropertyNames())
			hbaseConfig.put(name, appArgs.getProperties().getProperty(name));
		System.setProperty("HADOOP_USER_NAME", "devuser301");
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
		builder.setSpout(Constants.KAFKA_SPOUT_ID, KafkaSpoutFactory.getKafkaSpout(appArgs), 2);
		builder.setBolt(Constants.FILTER_BOLT_ID, new TwitterRawJsonConvertorBolt())
				.shuffleGrouping(Constants.KAFKA_SPOUT_ID);
		
		builder.setBolt(Constants.HDFS_BOLT_ID, HdfsBoltBuilder.build(appArgs))
				.shuffleGrouping(Constants.FILTER_BOLT_ID);
		builder.setBolt(Constants.HBASE_BOLT_ID,
				HBaseBoltBuilder.build(appArgs.getProperty(Constants.HBASE_TABLENAME), "hbaseConfig"))
				.shuffleGrouping(Constants.FILTER_BOLT_ID);

		builder.setBolt("USERS_MAX_FOLLOWERS", new UsersWithMaxFollowers()).shuffleGrouping(Constants.FILTER_BOLT_ID);
		builder.setBolt("TOPN_USERS_MAX_FOLLOWERS", new TopNUsersWithMaxFollowers())
				.globalGrouping("USERS_MAX_FOLLOWERS");

		builder.setBolt("LOCATION_BY_TWEETS", new LocationByTweets()).shuffleGrouping(Constants.FILTER_BOLT_ID);
		builder.setBolt("TOPN_LOCATION_BY_TWEETS", new TopNLocationByTweets()).globalGrouping("LOCATION_BY_TWEETS");

		builder.setBolt("MYSQL_WRITER", new MySQLDataWriterBolt(),2)
				.fieldsGrouping("TOPN_USERS_MAX_FOLLOWERS", new Fields("tableName"))
				.fieldsGrouping("TOPN_LOCATION_BY_TWEETS", new Fields("tableName"));
		return builder.createTopology();
	}

}
