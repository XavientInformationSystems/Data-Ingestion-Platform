package com.xavient.dip.storm.spout.kafka;

import com.xavient.dip.common.AppArgs;
import com.xavient.dip.common.config.DiPConfiguration;

import storm.kafka.KafkaSpout;



public class KafkaSpoutFactory {

	public static KafkaSpout getKafkaSpout(AppArgs appArgs) {
		return KafkaInputSpout.getKafkaSpout(appArgs.getProperty(DiPConfiguration.KAFKA_TOPIC),
				appArgs.getProperty(DiPConfiguration.ZK_HOST), appArgs.getProperty(DiPConfiguration.ZK_PORT, "2181"),
				appArgs.isRewind());
	}
}
