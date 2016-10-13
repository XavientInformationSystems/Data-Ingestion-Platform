package com.xavient.dip.spark.twitter;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.xavient.dip.spark.writer.SparkJdbcSourceWriter;

import scala.Tuple2;

public class TopNLocationByTweets extends TopN<String, Integer> {

	public TopNLocationByTweets(SparkJdbcSourceWriter rdbmsWriter,int topN) {
		super(rdbmsWriter,topN);
		this.tableName = "tweets_location";
		this.schema = new StructType(new StructField[] { new StructField("location", DataTypes.StringType, false, null),
				new StructField("count", DataTypes.IntegerType, false, null) });
	}

	@Override
	protected <T> JavaPairRDD<String, Integer> doMapToPair(JavaRDD<T> rdd) {
		return rdd.mapToPair(tweet -> {
			Object[] data = (Object[]) tweet;
			return new Tuple2<String, Integer>((String) data[8], 1);
		}).reduceByKey((val1, val2) -> val1 + val2);
	}

}
