package com.xavient.dip.storm.spout.kafka;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class KafkaInputSpout {

  public static KafkaSpout getKafkaSpout(String topic, String zkHost, String zkPort, Boolean rewind) {
  	String zkRoot = "/" + topic;
  	String zkSpoutId = topic;
  	BrokerHosts hosts = new ZkHosts(zkHost + ":" + zkPort);
  	SpoutConfig spoutCfg = new SpoutConfig(hosts, topic, zkRoot, zkSpoutId);
  	if (rewind) {
  		spoutCfg.ignoreZkOffsets = true;
  		//spoutCfg.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
  	}
  	spoutCfg.scheme = new SchemeAsMultiScheme(new StringScheme());
  	KafkaSpout kafkaSpout = new KafkaSpout(spoutCfg);
  	return kafkaSpout;
  }


}
