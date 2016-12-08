package com.xavient.dip.common.utils;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FlatJsonConverter {
	
	private FlatJsonConverter()
	{
		
	}

	public static Map<String, Object> convertToMap(String json) {
		final  Logger logger = LoggerFactory.getLogger(FlatJsonConverter.class);
		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> map = null;
		// convert JSON string to Map
		try {
			map = mapper.readValue(json, new TypeReference<LinkedHashMap<String, Object>>() {
			});
		} catch (IOException e) {
			logger.error("exception raised",e);

		}
		return map;
	}
	
	
	public static Object[] convertToValuesArray(String json) {
		return convertToMap(json).values().toArray();
	}
}
