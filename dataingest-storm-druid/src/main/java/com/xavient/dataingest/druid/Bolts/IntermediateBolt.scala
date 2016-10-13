package com.xavient.dataingest.druid.Bolts

import java.{ util => ju }
import backtype.storm.task.OutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple
import backtype.storm.tuple.Fields
import scala.collection.JavaConverters._
import com.xavient.dataingest.druid.Utils.JSonParser
import java.{util => ju}
import java.{util => ju}
import java.{util => ju}

class IntermidiateBolt extends BaseRichBolt {

  var _collector: OutputCollector = null

  @Override
  def prepare(stormConf: ju.Map[_, _], context: TopologyContext, collector: OutputCollector) {
    this._collector = collector;

  }

  @Override
  def execute(input: Tuple) {

    val value: ju.List[Object] = JSonParser.parse(input.getString(0))
    //println(value.get(0).)
    this._collector.emit(value)
    _collector.ack(input);

  }

  @Override
  def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields("event"))
  }

}
