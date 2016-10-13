package com.xavient.dataingest.storm.bolt.hbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;

import com.xavient.dataingest.storm.constants.Constants;
import com.xavient.dataingest.storm.vo.AppArgs;

import backtype.storm.tuple.Fields;


public class HBaseBoltFactory {

	public static HBaseBolt getHBaseBolt(String tablename, String rowKey, List<String> fields,
			List<String> counterFields, String columnFamily) {
		SimpleHBaseMapper mapper = new SimpleHBaseMapper().withRowKeyField(rowKey).withColumnFields(new Fields(fields))
				.withCounterFields(new Fields(counterFields)).withColumnFamily(columnFamily);
		return new HBaseBolt(tablename, mapper);
	}
	
	/**
   * Initializes HBase bolt with application configurations
   * @param appArgs Application configurations
   * @return Returns HBase bolt initialized with Application Configurations
   */
  public static HBaseDumpBolt getHBaseBolt(AppArgs appArgs) {
    String hbaseXmlLocation = appArgs.getProperty(Constants.HBASE_CONFIG_FILE);
    String tableName = appArgs.getProperty(Constants.HBASE_TABLENAME);
    String rowKeyCheck = appArgs.getProperty(Constants.HBASE_ROW_KEY_CHECK);
    return new HBaseDumpBolt(hbaseXmlLocation, tableName, rowKeyCheck, getColumnFamilies(appArgs),
        getColumns(appArgs));
  }

  /**
   * Gets HBase table columns from application configurations
   * @param appArgs
   * @return Returns HBase table columns
   */
  private static List<List<String>> getColumns(AppArgs appArgs) {
    String delimiter = Constants.DELIMITER_PREFIX + appArgs.getProperty(Constants.HBASE_COL_DELIMITER);
    String fdelimiter = Constants.DELIMITER_PREFIX + appArgs.getProperty(Constants.HBASE_COL_FAM_DELIMITER);
    String colNamesWithFamily = appArgs.getProperty(Constants.HBASE_COL_NAMES);
    List<List<String>> colNames = new ArrayList<>();
    for (String cols : colNamesWithFamily.split(fdelimiter)) {
      colNames.add(Arrays.asList(cols.split(delimiter)));
    }
    return colNames;
  }

  private static List<String> getColumnFamilies(AppArgs appArgs) {
    String cdelimiter = Constants.DELIMITER_PREFIX + appArgs.getProperty(Constants.HBASE_COL_DELIMITER);
    List<String> colNames = Arrays.asList(appArgs.getProperty(Constants.HBASE_COL_FAMILIES).split(cdelimiter));
    return colNames;
  }
}
