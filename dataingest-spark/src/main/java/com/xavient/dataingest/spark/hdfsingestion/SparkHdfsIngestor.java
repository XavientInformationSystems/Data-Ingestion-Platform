/* This class contains the method to write the data to HDFS */

package com.xavient.dataingest.spark.hdfsingestion;

import org.apache.spark.streaming.api.java.JavaDStream;

import com.xavient.dataingest.spark.constants.Constants;
import com.xavient.dataingest.spark.util.AppArgs;
import com.xavient.dataingest.spark.util.DataPayload;

public class SparkHdfsIngestor {

	@SuppressWarnings("deprecation")
	public static void hdfsDataWriter(JavaDStream<DataPayload> dp, AppArgs appArgs) {

		dp.foreachRDD(rdd -> {
			try {
				DataPayload str = rdd.first();
				rdd.saveAsTextFile(appArgs.getProperty(Constants.CLUSTER_FS_URL)
						+ appArgs.getProperty(Constants.HDFS_OUTPUT_PATH) + System.currentTimeMillis());

			} catch (Exception e) {

				System.out.println("");
			}
		});
	}
}
