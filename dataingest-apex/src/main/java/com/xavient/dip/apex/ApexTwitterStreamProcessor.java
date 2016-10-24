package com.xavient.dip.apex;

import java.util.Map.Entry;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.xavient.dip.apex.operator.HBaseSinkOperator;
import com.xavient.dip.apex.operator.HdfsSinkOperator;
import com.xavient.dip.apex.operator.TwitterStreamOperator;
import com.xavient.dip.common.config.DiPConfiguration;
import com.xavient.dip.common.utils.PropertyReader;

@ApplicationAnnotation(name = "ApexTwitterStreamProcessing")
public class ApexTwitterStreamProcessor implements StreamingApplication {
	private static final Logger logger = LoggerFactory.getLogger(ApexTwitterStreamProcessor.class);

	@Override
	public void populateDAG(DAG dag, Configuration config) {

		KafkaSinglePortInputOperator in = dag.addOperator("kafkaIn", new KafkaSinglePortInputOperator());
		TwitterStreamOperator tweeterStream = dag.addOperator("tweeterStream", TwitterStreamOperator.class);
		HdfsSinkOperator hdfsSinkOperator = dag.addOperator("hdfsSinkOperator", HdfsSinkOperator.class);
		hdfsSinkOperator.setFileName(config.get(DiPConfiguration.HDFS_FILE_NAME));

		HBaseSinkOperator hBaseSinkOperator = dag.addOperator("hBaseSinkOperator",new HBaseSinkOperator(config));
		hBaseSinkOperator.getStore().setTableName(config.get(DiPConfiguration.HBASE_TABLE_NAME));
		hBaseSinkOperator.getStore().setZookeeperQuorum(config.get(DiPConfiguration.ZK_HOST));
		hBaseSinkOperator.getStore().setZookeeperClientPort(config.getInt(DiPConfiguration.ZK_PORT, 2181));

		dag.addStream("kafkaStream", in.outputPort, tweeterStream.inputPort).setLocality(Locality.CONTAINER_LOCAL);
		dag.addStream("hdfsStream", tweeterStream.hdfsOutputPort, hdfsSinkOperator.input);
		dag.addStream("hbaseStream", tweeterStream.hBaseOutputPort, hBaseSinkOperator.input);

	}

	public void addConfiguration(Configuration config) {
		try {
			PropertyReader propertyReader = new PropertyReader(config.get(DiPConfiguration.CONFIG_FILE));
			for (Entry<Object, Object> entry : propertyReader.getProperties().entrySet()) {
				config.set(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
			}
		} catch (Exception e) {
			logger.error("Exception occured while reading application configuration", e);
		}
	}
}
