/* This class contains the method to write the data to HDFS */

package com.xavient.dip.spark.hdfs.ingestion;

import org.apache.spark.streaming.api.java.JavaDStream;

import com.xavient.dip.spark.constants.Constants;
import com.xavient.dip.spark.util.AppArgs;
import com.xavient.dip.spark.util.DataPayload;

public class SparkHdfsIngestor {

	public static void hdfsDataWriter(JavaDStream<DataPayload> dp, AppArgs appArgs) {
		dp.foreachRDD(rdd -> {
			try {
				rdd.saveAsTextFile(appArgs.getProperty(Constants.CLUSTER_FS_URL)
						+ appArgs.getProperty(Constants.HDFS_OUTPUT_PATH) + System.currentTimeMillis());
			} catch (Exception e) {
				System.out.println("");
			}
		});
	}
}
