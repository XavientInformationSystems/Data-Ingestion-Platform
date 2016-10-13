package com.xavient.dataingest.druid.Spouts
import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;


object  KafkaInputSpout {
  
  def getKafkaSpout (topic:String ,zkHost:String,zkPort:String,rewind:Boolean) : KafkaSpout= 
  {
  	val zkRoot = "/" + topic;
  	val zkSpoutId = topic;
  	val hosts = new ZkHosts(zkHost + ":" + zkPort);
  	val spoutCfg = new SpoutConfig(hosts, topic, zkRoot, zkSpoutId);
  	if (rewind) {
  		spoutCfg.ignoreZkOffsets = true;
  		//spoutCfg.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
  	}
  	spoutCfg.scheme = new SchemeAsMultiScheme(new StringScheme());
  	val kafkaSpout = new KafkaSpout(spoutCfg);
  	return kafkaSpout;
  }
  
  
}