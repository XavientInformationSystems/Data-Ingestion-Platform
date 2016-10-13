package com.xavient.dataingest.storm.builder;

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

import com.xavient.dataingest.storm.bolt.DataIngestionHdfsBolt;
import com.xavient.dataingest.storm.constants.Constants;
import com.xavient.dataingest.storm.vo.AppArgs;

public class HdfsBoltBuilder {

	public static HdfsBolt build(AppArgs appArgs) {
		RecordFormat format = new DelimitedRecordFormat()
				.withFieldDelimiter(appArgs.getProperty(Constants.HDFS_OUTPUT_DELIMITER));
		SyncPolicy syncPolicy = new CountSyncPolicy(
				Integer.valueOf(appArgs.getProperty(Constants.HDFS_SYNC_POLICY, "1000")));
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(
				Float.valueOf(appArgs.getProperty(Constants.FILE_SIZE_ROTATION)), Units.MB);
		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPrefix("Twitter")
				.withPath(appArgs.getProperty(Constants.HDFS_OUTPUT_PATH));
		HdfsBolt bolt = new DataIngestionHdfsBolt().withFsUrl(appArgs.getProperty(Constants.CLUSTER_FS_URL))
				.withFileNameFormat(fileNameFormat).withRecordFormat(format).withRotationPolicy(rotationPolicy)
				.withSyncPolicy(syncPolicy);
		return bolt;
	}
}
