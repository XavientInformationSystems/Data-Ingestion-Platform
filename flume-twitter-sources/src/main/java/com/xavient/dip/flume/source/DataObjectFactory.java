package com.xavient.dip.flume.source;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.gson.Gson;

import twitter4j.Status;

public class DataObjectFactory {

	private static Gson gson = new Gson();
	private static DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

	public static String getRawJSON(Status status) {
		Map<String, Object> data = new LinkedHashMap<String, Object>(15);
		data.put("timestamp", fmt.print(status.getCreatedAt().getTime()));
		data.put("id", status.getId());
		data.put("text", status.getText());
		data.put("source", status.getSource());
		data.put("reTweeted", status.isRetweet());
		data.put("username", status.getUser().getName());
		data.put("createdAt", status.getCreatedAt());
		data.put("retweetCount", status.getRetweetCount());
		data.put("userLocation",
				StringUtils.isNotBlank(status.getUser().getLocation()) ? status.getUser().getLocation() : "unknown");
		data.put("inReplyToUserId", status.getInReplyToUserId());
		data.put("inReplyToStatusId", status.getInReplyToStatusId());
		data.put("userScreenName", status.getUser().getScreenName());
		data.put("userDescription",
				StringUtils.isNotBlank(status.getUser().getDescription()) ? status.getUser().getDescription() : "");
		data.put("userFriendsCount", status.getUser().getFriendsCount());
		data.put("userStatusesCount", status.getUser().getStatusesCount());
		data.put("userFollowersCount", status.getUser().getFollowersCount());
		return gson.toJson(data);
	}

}
