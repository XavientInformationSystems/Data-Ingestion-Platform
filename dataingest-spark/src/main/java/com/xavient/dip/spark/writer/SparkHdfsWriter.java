package com.xavient.dip.spark.writer;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.xavient.dip.common.AppArgs;
import com.xavient.dip.common.config.DiPConfiguration;

public class SparkHdfsWriter {

	public static <T> void write(JavaDStream<T> javaDStream, AppArgs appArgs) {
		javaDStream.foreachRDD(rdd -> {
			rdd.map(record -> {
				StringBuilder recordBuilder = new StringBuilder();
				
				for (Object e : (Object[]) record) {
					recordBuilder.append(e);
					recordBuilder.append(appArgs.getProperty(DiPConfiguration.HDFS_OUTPUT_DELIMITER));
				}
				return StringUtils.removeEnd(recordBuilder.toString(),
						appArgs.getProperty(DiPConfiguration.HDFS_OUTPUT_DELIMITER));
			}).saveAsTextFile(appArgs.getProperty(DiPConfiguration.CLUSTER_FS_URL)
					+ appArgs.getProperty(DiPConfiguration.HDFS_OUTPUT_PATH) + System.currentTimeMillis());
		});
	}
}
