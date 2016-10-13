package com.xavient.dip.spark.util;

import java.util.ArrayList;
import java.util.List;

public class ProcessTSVStream {

	public List<Object> tsvElements = new ArrayList<Object>();

	public List<List<Object>> tsvData = new ArrayList<List<Object>>();

	public List<List<Object>> getTSVData(String tsvString) {

		try {

			String parts[] = tsvString.split("\t");

			for (String items : parts) {
				tsvElements.add(items);

			}
			tsvData.add(tsvElements);

		} catch (Exception e) {

		}
		return tsvData;
	}
}