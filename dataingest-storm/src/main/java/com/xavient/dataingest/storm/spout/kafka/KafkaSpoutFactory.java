package com.xavient.dataingest.storm.spout.kafka;

import com.xavient.dataingest.storm.constants.Constants;
import com.xavient.dataingest.storm.vo.AppArgs;

import storm.kafka.KafkaSpout;

public class KafkaSpoutFactory {

	public static KafkaSpout getKafkaSpout(AppArgs appArgs) {
    return KafkaInputSpout.getKafkaSpout(appArgs.getProperty(Constants.KAFKA_TOPIC),
        appArgs.getProperty(Constants.ZK_HOST), appArgs.getProperty(Constants.ZK_PORT, "2181"),
        appArgs.isRewind());
  }
}
