package com.xavient.dataingest.storm;


import com.xavient.dataingest.storm.bolt.filter.FilterInputBolt;
import com.xavient.dataingest.storm.bolt.hbase.HBaseBoltFactory;
import com.xavient.dataingest.storm.bolt.hdfs.HdfsBoltFactory;
import com.xavient.dataingest.storm.constants.Constants;
import com.xavient.dataingest.storm.exception.DataIngestException;
import com.xavient.dataingest.storm.spout.kafka.KafkaSpoutFactory;
import com.xavient.dataingest.storm.util.CmdLineParser;
import com.xavient.dataingest.storm.vo.AppArgs;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class DataIngestionTopology {

	private static final String TOPOLOGY_NAME = "DataIngestion";

	/**
	 * Main method for Storm topology submit.
	 * 
	 * @param args
	 * @throws DataIngestException
	 */
	public static void main(String[] args) throws DataIngestException {
		DataIngestionTopology topology = new DataIngestionTopology();
		Config conf = new Config();
		CmdLineParser parser = new CmdLineParser();
		AppArgs appArgs = parser.validateArgs(args);

		// LocalCluster local = new LocalCluster();
		// local.submitTopology(TOPOLOGY_NAME, conf,
		// topology.buildTopology(appArgs));
		try {
			StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, topology.buildTopology(appArgs));
		} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
			throw new DataIngestException(e.getMessage());
		}
	}

	/**
	 * Builds Storm topology.
	 * 
	 * @param appArgs
	 *            Application configurations
	 * @return Returns Storm topology with Spouts and Bolts
	 */
	private StormTopology buildTopology(AppArgs appArgs) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(Constants.KAFKA_SPOUT_ID, KafkaSpoutFactory.getKafkaSpout(appArgs), 1);
		builder.setBolt(Constants.FILTER_BOLT_ID, new FilterInputBolt()).shuffleGrouping(Constants.KAFKA_SPOUT_ID);
		builder.setBolt(Constants.HDFS_BOLT_ID, HdfsBoltFactory.getHDFSBolt(appArgs))
				.shuffleGrouping(Constants.FILTER_BOLT_ID);
		builder.setBolt(Constants.HBASE_BOLT_ID, HBaseBoltFactory.getHBaseBolt(appArgs))
				.shuffleGrouping(Constants.FILTER_BOLT_ID);
		return builder.createTopology();
	}

}
