/* This is the main class */

package com.xavient.dataingest.spark.main;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;
import com.xavient.dataingest.spark.constants.Constants;
import com.xavient.dataingest.spark.exception.DataIngestException;
import com.xavient.dataingest.spark.hdfsingestion.SparkHdfsIngestor;
import com.xavient.dataingest.spark.util.AppArgs;
import com.xavient.dataingest.spark.util.CmdLineParser;
import com.xavient.dataingest.spark.util.DataPayload;
import com.xavient.dataingest.spark.util.MetadataParser;

import scala.Tuple2;

public class SparkIngestion implements Serializable {

	private static final long serialVersionUID = 3289368866519818229L;

	public static void main(String[] args) throws ParserConfigurationException, IOException, DataIngestException {

		CmdLineParser cmdLineParser = new CmdLineParser();
		final AppArgs appArgs = cmdLineParser.validateArgs(args);

		System.setProperty("HADOOP_USER_NAME", appArgs.getProperty(Constants.HDFS_USER_NAME));

		SparkConf conf = new SparkConf().setAppName("SparkStreamingTest")
				.setMaster(appArgs.getProperty(Constants.SPARK_MASTER_URL));
		JavaSparkContext jsc = new JavaSparkContext(conf);

		try {

			JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(500));

			JavaPairReceiverInputDStream<String, String> stream = KafkaUtils.createStream(jssc,
					appArgs.getProperty(Constants.ZK_HOST) + ":" + appArgs.getProperty(Constants.ZK_PORT), "group",
					getKafkaTopics(appArgs));

			JavaDStream<String> lines = stream.map(tuple -> tuple._2);

			JavaDStream<DataPayload> dataPayLoadDStream = payloadIngestor(lines);

			JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, getConf(appArgs));

			hbaseContext.streamBulkPut(dataPayLoadDStream,
					TableName.valueOf(appArgs.getProperty(Constants.HBASE_TABLENAME)),
					new Function<DataPayload, Put>() {

						private static final long serialVersionUID = -7696995553026962751L;

						@Override
						public Put call(DataPayload v) throws Exception {

							Put put = null;
							String payload = v.toString();
							String[] columnnames = appArgs.getProperty(Constants.HBASE_COL_NAMES).split("\\|");

							String[] ls = payload.split("\n");

							int index = 0;

							for (String s : ls) {

								String[] individualitems = s.split("\\|");

								put = new Put(
										Bytes.toBytes(Long.toString(System.currentTimeMillis()) + individualitems[0]));
								for (String cn : columnnames) {
									put.addColumn(Bytes.toBytes(appArgs.getProperty(Constants.HBASE_COL_FAMILIES)),
											Bytes.toBytes(cn), Bytes.toBytes(individualitems[index]));

									index++;

								}

								index = 0;
							}

							return put;
						}

					});

			SparkHdfsIngestor.hdfsDataWriter(dataPayLoadDStream, appArgs);

			jssc.start();
			jssc.awaitTermination();

		} finally {
			jsc.stop();

		}
	}

	private static Configuration getConf(AppArgs appArgs) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master",
				appArgs.getProperty(Constants.ZK_HOST) + ":" + appArgs.getProperty(Constants.HBASE_MASTER));
		conf.set("timeout", "120000");
		conf.set("hbase.zookeeper.quorum",
				appArgs.getProperty(Constants.ZK_HOST) + ":" + appArgs.getProperty(Constants.ZK_PORT));
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");

		HBaseAdmin admin;
		try {
			admin = new HBaseAdmin(conf);

			if (!admin.tableExists(appArgs.getProperty(Constants.HBASE_TABLENAME))) {
				TableName tableName = TableName.valueOf(appArgs.getProperty(Constants.HBASE_TABLENAME));
				HTableDescriptor htd = new HTableDescriptor(tableName);
				HColumnDescriptor hcd = new HColumnDescriptor(appArgs.getProperty(Constants.HBASE_COL_FAMILIES));
				htd.addFamily(hcd);
				admin.createTable(htd);
			}
		} catch (IOException e) {

			e.printStackTrace();
		}

		return conf;
	}

	private static Map<String, Integer> getKafkaTopics(AppArgs appArgs) {
		Map<String, Integer> topics = new HashMap<String, Integer>();
		topics.put(appArgs.getProperty(Constants.KAFKA_TOPIC), 1);
		return topics;
	}

	private static JavaDStream<DataPayload> payloadIngestor(JavaDStream<String> lines) {
		JavaDStream<DataPayload> dataPayLoadDStream = lines.map(new Function<String, DataPayload>() {

			private static final long serialVersionUID = 8246041326936027578L;

			@Override
			public DataPayload call(String input) throws Exception {
				DataPayload dataPayload = new DataPayload();
				MetadataParser dataParser = new MetadataParser();
				List<List<Object>> data = dataParser.parse(input);
				for (List<Object> lo : data) {
					String payload = "";
					for (Object o : lo) {
						payload = payload + "|" + (String) o;

					}
					dataPayload.payload.add(payload.substring(1, payload.length()));
				}

				return dataPayload;
			}

		});
		return dataPayLoadDStream;
	}
}