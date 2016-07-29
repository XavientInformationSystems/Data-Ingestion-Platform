package com.xavient.dataingest.apex.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessTSVStream {

	public List<Map<String, String>> tsvData = new ArrayList<Map<String, String>>();

	public List<Map<String, String>> getTSVData(String tsvString) {
		Map<String, String> map = new HashMap<String, String>();
		String[] bookdata = tsvString.split("\t");

		String columnNames = "id|author|title|genre|price|publish_date|description";
		String[] col = columnNames.split("\\|");

		for (int i = 0; i < col.length - 1; i++) {
			map.put(col[i], bookdata[i]);

		}

		tsvData.add(map);
		return tsvData;
	}
}