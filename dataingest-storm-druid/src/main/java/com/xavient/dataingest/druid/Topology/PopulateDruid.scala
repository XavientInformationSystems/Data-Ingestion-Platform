package com.xavient.dataingest.druid.Topology


import backtype.storm.topology.TopologyBuilder
import com.metamx.tranquility.storm.BeamBolt
import backtype.storm.generated.StormTopology
import backtype.storm.Config
import backtype.storm.StormSubmitter
import com.xavient.dataingest.druid.Spouts.KafkaSpoutFactory
import com.xavient.dataingest.druid.Bolts
import com.xavient.dataingest.druid.Bolts.IntermidiateBolt
import com.xavient.dataingest.druid.Bolts.DruidBeamFactory

object PopulateDruid extends App {

  //    val populateDruid = new PopulateDruid

  val conf = new Config();
  StormSubmitter.submitTopology("DruidInsertion", conf, buildTopology());

  def buildTopology(): StormTopology = {
    val builder = new TopologyBuilder();

    builder.setSpout("kafkaSpout", KafkaSpoutFactory.getKafkaSpout(), 1)
    val bolt = new BeamBolt(new DruidBeamFactory)
    builder.setBolt("jsonMapSpout", new IntermidiateBolt).shuffleGrouping("kafkaSpout");
    builder.setBolt("sendToDruid", bolt).shuffleGrouping("jsonMapSpout");
    builder.createTopology();
  }

}