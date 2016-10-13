package com.xavient.dataingest.druid.Spouts

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import com.xavient.dataingest.druid.Utils.Constants
import com.xavient.dataingest.druid.Utils.Property

object KafkaSpoutFactory {

  def getKafkaSpout(): KafkaSpout = {
    return KafkaInputSpout.getKafkaSpout(Property.getProperty(Constants.KAFKA_TOPIC),
      Property.getProperty(Constants.KAFKA_ZOOKEEPER), Property.getProperty(Constants.KAFKA_PORT), false);

  }

}