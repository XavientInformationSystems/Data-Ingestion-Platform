package com.xavient.dip.spark.twitter;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.xavient.dip.spark.writer.SparkJdbcSourceWriter;

import scala.Tuple2;

public class TopNUsersWithMaxFollowers extends TopN<String, Integer> {

	public TopNUsersWithMaxFollowers(SparkJdbcSourceWriter rdbmsWriter,int topN) {
		super(rdbmsWriter,topN);
		this.tableName = "user_followers";
		this.schema = new StructType(new StructField[] { new StructField("username", DataTypes.StringType, false, null),
				new StructField("count", DataTypes.IntegerType, false, null) });
	}

	@Override
	protected <T> JavaPairRDD<String, Integer> doMapToPair(JavaRDD<T> rdd) {
		return rdd.mapToPair(tweet -> {
			Object[] data = (Object[]) tweet;
			
			return new Tuple2<String, Integer>((String) data[5], (Integer) data[15]);
		});
	}
}
