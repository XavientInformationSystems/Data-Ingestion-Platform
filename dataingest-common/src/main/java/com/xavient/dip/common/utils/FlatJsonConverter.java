package com.xavient.dip.common.utils;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class FlatJsonConverter {

	public static Map<String, Object> convertToMap(String json) {
		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> map = null;
		// convert JSON string to Map
		try {
			map = mapper.readValue(json, new TypeReference<LinkedHashMap<String, Object>>() {
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
		return map;
	}
	
	
	public static Object[] convertToValuesArray(String json) {
		return convertToMap(json).values().toArray();
	}
}
