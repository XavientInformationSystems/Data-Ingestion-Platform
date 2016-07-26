package com.xavient.dataingest.spark.hdfsingestion;

import org.apache.spark.streaming.api.java.JavaDStream;

import com.xavient.dataingest.spark.constants.Constants;
import com.xavient.dataingest.spark.util.AppArgs;
import com.xavient.dataingest.spark.util.DataPayload;

public class SparkHdfsIngestor {
	
	public static void hdfsDataWriter(JavaDStream<DataPayload> dp ,AppArgs appArgs ) {

		dp.dstream().saveAsTextFiles(
				appArgs.getProperty(Constants.CLUSTER_FS_URL) + appArgs.getProperty(Constants.CLUSTER_FS_PATH), "dir");
	}
}
