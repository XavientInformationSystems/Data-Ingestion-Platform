package com.xavient.dataingest.apex.operators;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.contrib.hbase.AbstractHBaseWindowPutOutputOperator;
import com.datatorrent.netlet.util.DTThrowable;
import com.xavient.dataingest.apex.constants.Constants;

public class HBasePutOperator extends AbstractHBaseWindowPutOutputOperator<Map<String, String>> {

  private static final transient Logger logger = LoggerFactory.getLogger(HBasePutOperator.class);

  @Override
  public Put operationPut(Map<String, String> map) {
    if (logger.isDebugEnabled()) {
      logger.debug("Got row :: {}", map);
    }
    try {
      return parseMap(map);
    } catch (Exception e) {
      DTThrowable.rethrow(e);
      return null;
    }
  }

  public Put parseMap(Map<String, String> map) {
    logger.debug("Input Map: {}", map);
    Put put = new Put((System.currentTimeMillis() + map.get(Constants.ROW_KEY)).getBytes());
    for (Entry<String, String> entry : map.entrySet()) {
      put.addColumn(Constants.COLUMN_FAMILY.getBytes(), entry.getKey().getBytes(), entry.getValue().getBytes());
    }
    return put;
  }

}
