package com.xavient.dataingest.apex;

import java.io.IOException;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.netlet.util.DTThrowable;
import com.xavient.dataingest.apex.constants.Constants;
import com.xavient.dataingest.apex.operators.ClassifierOperator;
import com.xavient.dataingest.apex.operators.FileOutputOperator;
import com.xavient.dataingest.apex.operators.HBasePutOperator;
import com.xavient.dataingest.apex.util.HBaseUtil;

@ApplicationAnnotation(name = "ApexStreamProcessing")
public class ApexStreamProcessor implements StreamingApplication {
  
  private static final transient Logger logger = LoggerFactory.getLogger(ApexStreamProcessor.class);

  @Override
  public void populateDAG(DAG dag, Configuration conf) {
    KafkaSinglePortInputOperator in = dag.addOperator("kafkaIn", new KafkaSinglePortInputOperator());
    ClassifierOperator classifierOut = dag.addOperator("classifierOut", ClassifierOperator.class);
    FileOutputOperator fileOut = dag.addOperator("fileOut", FileOutputOperator.class);
    ConsoleOutputOperator cons = dag.addOperator("consoleOut", ConsoleOutputOperator.class);
    HBasePutOperator hbaseOut = dag.addOperator("hbaseOut", HBasePutOperator.class);

    hbaseOut.getStore().setTableName(Constants.TABLE_NAME);
    hbaseOut.getStore().setZookeeperQuorum(Constants.ZK_QUORUM);
    hbaseOut.getStore().setZookeeperClientPort(Constants.ZK_PORT);
    fileOut.setFileName(Constants.FILE_NAME);
    
    preprocess(hbaseOut);

    dag.addStream("kafkaConsoleStream", in.outputPort, classifierOut.input).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("fileStream", classifierOut.strTupleOutPort, fileOut.input, cons.input);
    dag.addStream("hbaseStream", classifierOut.mapTupleOutPort, hbaseOut.input);
  }

  private void preprocess(HBasePutOperator hbaseOut) {
    try {
      logger.info("Checking HBase table: '{}'", Constants.TABLE_NAME);
      HBaseUtil.createTable(HBaseConfiguration.create(), Constants.TABLE_NAME, Constants.COLUMN_FAMILY);
      logger.info("HBase table: '{}' created.", Constants.TABLE_NAME);
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

}
