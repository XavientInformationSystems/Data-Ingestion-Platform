/*
 *  Main class which submits the flink program to the cluster . 
 *  It also creates the sink and the source  
 */
package com.xavient.dataingest.flink;

import java.util.List;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.DateTimeBucketer;
import org.apache.flink.streaming.connectors.fs.RollingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import com.xavient.dataingest.flink.constants.Constants;
import com.xavient.dataingest.flink.hbase.HBaseOutputFormat;
import com.xavient.dataingest.flink.util.CmdLineParser;
import com.xavient.dataingest.flink.util.MetadataParser;
import com.xavient.dataingest.flink.vo.AppArgs;

/**
 * Main Class For FlinkStreaming
 *
 */
public class FlinkStreamProcessor {
	public static void main(String[] args) throws Exception {

		CmdLineParser parser = new CmdLineParser();
		AppArgs appArgs = parser.validateArgs(args);

		// This line is used to submit the flink job to the environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("10.5.3.166", 35253,
				"D:/Projects/FlinkStreaming/FlinkStreaming/target/uber-dataingest-1.0.0.jar");

		/*
		 * Uncomment the below line and comment the above if submitting the job
		 * through UI
		 */
		// StreamExecutionEnvironment env =
		// StreamExecutionEnvironment.getExecutionEnvironment();

		env.enableCheckpointing(5000);
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers",
				appArgs.getProperty(Constants.KAFKA_HOST) + ":" + appArgs.getProperty(Constants.KAFKA_PORT));

		properties.setProperty("zookeeper.connect",
				appArgs.getProperty(Constants.ZK_HOST) + ":" + appArgs.getProperty(Constants.ZK_PORT));
		properties.setProperty("group.id", "test");

		// Creates a source from which flink program picks up the data
		DataStream<String> rides = env
				.addSource(new FlinkKafkaConsumer08<String>(appArgs.getProperty(Constants.FLINK_KAFKA_TOPIC),
						new SimpleStringSchema(), properties))
				.name("KafkaSource");

		DataStream<DataPayload> dp = rides.map(new MapFunction<String, DataPayload>() {

			private static final long serialVersionUID = -2149452291487827333L;
			DataPayload d = new DataPayload();

			public DataPayload map(String value) throws Exception {
				DataPayload d = new DataPayload();

				List<List<Object>> data = MetadataParser.parse(value);

				for (List<Object> lo : data) {
					String payload = "";
					for (Object o : lo) {
						payload = payload + "|" + (String) o;

					}
					d.payload.add(payload.substring(1, payload.length()));
				}

				return d;

			}

		}).name("Map Data");

		System.setProperty("HADOOP_USER_NAME", appArgs.getProperty(Constants.HADOOP_USER_NAME));
		//HDFS Sink to write the data to the HDFS
		RollingSink sink = new RollingSink<String>(appArgs.getProperty(Constants.FLINK_OUTPUT_PATH));
		sink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HH-mm-ss"));

		sink.setBatchSize(1024 * 1024 * 400);
		sink.setInProgressPrefix("flink");
		sink.setInProgressSuffix(".text");

		dp.addSink(sink).name("HDFS Sink");

		//FLink writes to the HBASE using the HBASE output format
		dp.writeUsingOutputFormat(new HBaseOutputFormat<DataPayload>()).name("HBASE Sink");

		env.execute();
	}
}
