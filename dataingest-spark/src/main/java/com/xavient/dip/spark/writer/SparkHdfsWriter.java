package com.xavient.dip.spark.writer;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.xavient.dip.spark.constants.Constants;
import com.xavient.dip.spark.util.AppArgs;

public class SparkHdfsWriter {

	public static <T> void write(JavaDStream<T> javaDStream, AppArgs appArgs) {
		javaDStream.foreachRDD(rdd -> {
			rdd.map(record -> {
				StringBuilder recordBuilder = new StringBuilder();
				for (Object e : (Object[]) record) {
					recordBuilder.append(e);
					recordBuilder.append(appArgs.getProperty("hdfs.output.delimiter"));
				}
				return StringUtils.removeEnd(recordBuilder.toString(), appArgs.getProperty("hdfs.output.delimiter"));
			}).saveAsTextFile(appArgs.getProperty(Constants.CLUSTER_FS_URL)
					+ appArgs.getProperty(Constants.HDFS_OUTPUT_PATH) + System.currentTimeMillis());
		});
	}
}
