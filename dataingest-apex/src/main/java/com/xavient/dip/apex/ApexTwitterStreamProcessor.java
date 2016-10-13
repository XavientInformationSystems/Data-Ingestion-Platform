package com.xavient.dip.apex;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.xavient.dataingest.apex.constants.Constants;
import com.xavient.dip.apex.operator.HBaseSinkOperator;
import com.xavient.dip.apex.operator.HdfsSinkOperator;
import com.xavient.dip.apex.operator.TwitterStreamOperator;

@ApplicationAnnotation(name = "ApexTwitterStreamProcessing")
public class ApexTwitterStreamProcessor implements StreamingApplication {

	@Override
	public void populateDAG(DAG dag, Configuration conf) {
		KafkaSinglePortInputOperator in = dag.addOperator("kafkaIn", new KafkaSinglePortInputOperator());
		TwitterStreamOperator tweeterStream = dag.addOperator("tweeterStream", TwitterStreamOperator.class);

		HdfsSinkOperator hdfsSinkOperator = dag.addOperator("hdfsSinkOperator", HdfsSinkOperator.class);
		hdfsSinkOperator.setFileName(Constants.FILE_NAME);

		HBaseSinkOperator hBaseSinkOperator = dag.addOperator("hBaseSinkOperator", HBaseSinkOperator.class);
		hBaseSinkOperator.getStore().setTableName(Constants.TABLE_NAME);
		hBaseSinkOperator.getStore().setZookeeperQuorum(Constants.ZK_QUORUM);
		hBaseSinkOperator.getStore().setZookeeperClientPort(Constants.ZK_PORT);

		dag.addStream("kafkaStream", in.outputPort, tweeterStream.inputPort).setLocality(Locality.CONTAINER_LOCAL);
		dag.addStream("hdfsStream", tweeterStream.hdfsOutputPort, hdfsSinkOperator.input);
		dag.addStream("hbaseStream", tweeterStream.hBaseOutputPort, hBaseSinkOperator.input);
	}

}
