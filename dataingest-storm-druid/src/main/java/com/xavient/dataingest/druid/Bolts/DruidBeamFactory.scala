package com.xavient.dataingest.druid.Bolts

import com.metamx.tranquility.druid.SpecificDruidDimensions
import com.metamx.tranquility.storm.BeamFactory
import backtype.storm.task.IMetricsContext
import com.metamx.tranquility.druid.DruidRollup
import com.metamx.tranquility.druid.DruidLocation
import com.metamx.tranquility.druid.DruidBeams
import com.metamx.tranquility.beam.ClusteredBeamTuning
import io.druid.query.aggregation.LongSumAggregatorFactory
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.BoundedExponentialBackoffRetry
import com.metamx.common.Granularity
import io.druid.granularity.QueryGranularity
import org.joda.time.Period
import scala.collection.mutable.HashMap
import com.xavient.dataingest.druid.Utils.Constants
import com.xavient.dataingest.druid.Utils.Property
import io.druid.query.aggregation.CountAggregatorFactory
import org.scala_tools.time.Imports.DateTime



class DruidBeamFactory extends BeamFactory[HashMap[String, Any]] {

  def makeBeam(conf: java.util.Map[_, _], metrics: IMetricsContext) = {

    val curator = CuratorFrameworkFactory.newClient(
      Property.getProperty(Constants.DRUID_ZK),
      new BoundedExponentialBackoffRetry(100, 1000, 5))
    curator.start()

    val indexService = Property.getProperty(Constants.DRUID_INDEXSERVICE)
    val discoveryPath = Property.getProperty(Constants.DRUID_DISCOVERYPATH)
    val dataSource = Property.getProperty(Constants.DRUID_DATASOURCE)
    val dimensions = Property.getProperty(Constants.DRUID_DIMENSIONS).split(",").toSeq
    //val aggregators = Seq(new   LongSumAggregatorFactory("total_deleted", "deleted") ,new LongSumAggregatorFactory("total_deleted", "deleted"))
    val aggregators = Seq(new CountAggregatorFactory("TotalTweets"))

    DruidBeams
      .builder((eventMap: HashMap[String, Any]) => new DateTime(eventMap("timestamp")))
      .curator(curator)
      .discoveryPath(discoveryPath)
      .location(DruidLocation.create(indexService, dataSource))
      .rollup(DruidRollup(SpecificDruidDimensions(dimensions), aggregators, QueryGranularity.MINUTE))
      .tuning(
        ClusteredBeamTuning(
          segmentGranularity = Granularity.HOUR,
          windowPeriod = new Period(Property.getProperty(Constants.DRUID_WINDOWPERIOD)),
          partitions = 1,
          replicants = 1))
      .buildBeam()

  }

}

