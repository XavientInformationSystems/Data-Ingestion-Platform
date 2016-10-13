package com.xavient.dip.spark.twitter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.xavient.dip.spark.writer.SparkJdbcSourceWriter;

import scala.Tuple2;

public abstract class TopN<K, V extends Comparable<V>> {
   
	protected int topN;
	protected String tableName;
	protected StructType schema;
	protected SparkJdbcSourceWriter rdbmsWriter;

	public TopN(SparkJdbcSourceWriter rdbmsWriter,int topN) {
		super();
		this.topN=topN;
		this.rdbmsWriter = rdbmsWriter;
	}

	public <T> void compute(JavaDStream<T> twitterStream) {
		twitterStream.foreachRDD(rdd -> {
			List<Row> rows = new ArrayList<>();
			doMapToPair(rdd).top(topN,
					(Comparator<Tuple2<K, V>> & Serializable) (tuple1, tuple2) -> tuple1._2.compareTo(tuple2._2))
					.forEach(tuple -> rows.add(createRow(tuple)));
			rdbmsWriter.write(rows, schema, tableName);
		});

	}

	protected Row createRow(Tuple2<K, V> record) {
		return RowFactory.create(record._1, record._2);
	}

	protected abstract <T> JavaPairRDD<K, V> doMapToPair(JavaRDD<T> rdd);

}
