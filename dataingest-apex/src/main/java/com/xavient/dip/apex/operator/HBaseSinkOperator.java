package com.xavient.dip.apex.operator;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.contrib.hbase.AbstractHBaseWindowPutOutputOperator;

public class HBaseSinkOperator extends AbstractHBaseWindowPutOutputOperator<Object[]> {

	private static final String columnFamily = "tweets";
	private static final String[] columnFields = { "text", "source", "reTweeted", "username", "createdAt",
			"retweetCount", "userLocation", "inReplyToUserId", "inReplyToStatusId", "userScreenName", "userDescription",
			"userFriendsCount", "userStatusesCount", "userFollowersCount" };

	@Override
	public Put operationPut(Object[] tuple) throws IOException {
		Put put = new Put(Bytes.toBytes(String.valueOf(tuple[1])));
		for (int i = 2; i < tuple.length; i++) {
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnFields[i - 2]),
					Bytes.toBytes(String.valueOf(tuple[i])));
		}
		return put;
	}

}
