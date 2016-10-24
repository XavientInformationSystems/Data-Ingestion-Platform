package com.xavient.dip.storm.builder;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import com.xavient.dip.common.AppArgs;
import com.xavient.dip.common.config.DiPConfiguration;
import com.xavient.dip.storm.bolt.DataIngestionHdfsBolt;


public class HdfsBoltBuilder {

	public static HdfsBolt build(AppArgs appArgs) {
		RecordFormat format = new DelimitedRecordFormat()
				.withFieldDelimiter(appArgs.getProperty(DiPConfiguration.HDFS_OUTPUT_DELIMITER));
		SyncPolicy syncPolicy = new CountSyncPolicy(
				Integer.valueOf(appArgs.getProperty(DiPConfiguration.HDFS_SYNC_POLICY, "1000")));
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(
				Float.valueOf(appArgs.getProperty(DiPConfiguration.FILE_SIZE_ROTATION)), Units.MB);
		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPrefix("Twitter")
				.withPath(appArgs.getProperty(DiPConfiguration.HDFS_OUTPUT_PATH));
		HdfsBolt bolt = new DataIngestionHdfsBolt().withFsUrl(appArgs.getProperty(DiPConfiguration.CLUSTER_FS_URL))
				.withFileNameFormat(fileNameFormat).withRecordFormat(format).withRotationPolicy(rotationPolicy)
				.withSyncPolicy(syncPolicy);
		return bolt;
	}
}
