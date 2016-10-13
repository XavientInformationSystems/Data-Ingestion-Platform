package com.xavient.dataingest.storm.builder;

import com.xavient.dataingest.storm.bolt.DataIngestionHBaseMapper;

import org.apache.storm.hbase.bolt.HBaseBolt;

import com.xavient.dataingest.storm.bolt.DataIngestionHBaseBolt;

import backtype.storm.tuple.Fields;

public class HBaseBoltBuilder {

	public static HBaseBolt build(String tableName, String configKey) {
		DataIngestionHBaseMapper hBaseMapper = new DataIngestionHBaseMapper().withColumnFamily("tweets")
				.withRowKeyField("id")
				.withColumnFields(new Fields("text", "source", "reTweeted", "username", "createdAt", "retweetCount",
						"userLocation", "inReplyToUserId", "inReplyToStatusId", "userScreenName", "userDescription",
						"userFriendsCount", "userStatusesCount", "userFollowersCount"));
		HBaseBolt baseBolt = new DataIngestionHBaseBolt(tableName, hBaseMapper);
		baseBolt.withConfigKey(configKey);
		return baseBolt;
	}
}
