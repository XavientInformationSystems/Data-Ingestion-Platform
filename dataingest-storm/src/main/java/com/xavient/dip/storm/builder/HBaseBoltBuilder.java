package com.xavient.dip.storm.builder;

import org.apache.storm.hbase.bolt.HBaseBolt;

import com.xavient.dip.common.AppArgs;
import com.xavient.dip.common.config.DiPConfiguration;
import com.xavient.dip.storm.bolt.DataIngestionHBaseBolt;
import com.xavient.dip.storm.bolt.DataIngestionHBaseMapper;

import backtype.storm.tuple.Fields;

public class HBaseBoltBuilder {

	public static HBaseBolt build(AppArgs appArgs, String configKey) {
		DataIngestionHBaseMapper hBaseMapper = new DataIngestionHBaseMapper()
				.withColumnFamily(appArgs.getProperty(DiPConfiguration.HBASE_COL_FAMILY))
				.withRowKeyField(appArgs.getProperty(DiPConfiguration.HBASE_ROW_KEY))
				.withColumnFields(new Fields(appArgs.getProperty(DiPConfiguration.HBASE_COL_NAMES)
						.split(appArgs.getProperty(DiPConfiguration.HBASE_COL_DELIMITER))));
		HBaseBolt baseBolt = new DataIngestionHBaseBolt(appArgs.getProperty(DiPConfiguration.HBASE_TABLE_NAME),
				hBaseMapper);
		baseBolt.withConfigKey(configKey);
		return baseBolt;
	}
}
