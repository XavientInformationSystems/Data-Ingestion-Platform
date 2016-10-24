package com.xavient.dip.storm.utils;

import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;

public class MapOrdering implements Comparator<Map.Entry<String, Integer>> {

	@Override
	public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
		return Integer.compare(o2.getValue(), o1.getValue());
	}

}
