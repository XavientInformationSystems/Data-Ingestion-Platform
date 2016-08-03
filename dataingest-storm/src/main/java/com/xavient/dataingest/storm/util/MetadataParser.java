package com.xavient.dataingest.storm.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.xavient.dataingest.storm.constants.Constants;
import com.xavient.dataingest.storm.exception.DataIngestException;

public class MetadataParser {

	private static final ObjectMapper m = new ObjectMapper();

	public static List<Object> parse(String str) throws DataIngestException {
		try {
			if ((str.startsWith("{"))) {
				JsonNode rootNode = m.readTree(str);
				return getMetadata(rootNode);
			} else if ((str.startsWith("<"))) {
				List<List<Object>> xmldata = new ProcessXMLStream().getXMLData(str);
				List<Object> data = new ArrayList<>();
				for (List<Object> object : xmldata) {
					for (Object obj : object) {
						data.add(obj);
					}
				}
				return data;
			} else {

				List<List<Object>> tsvdata = new ProcessTSVStream().getTSVData(str);
				List<Object> data = new ArrayList<>();
				for (List<Object> object : tsvdata) {
					for (Object obj : object) {
						data.add(obj);
					}
				}
				return data;

			}
		} catch (JsonProcessingException e) {
			throw new DataIngestException(e.getMessage());
		} catch (IOException e) {
			throw new DataIngestException(e.getMessage());
		} catch (ParserConfigurationException e) {
			throw new DataIngestException(e.getMessage());
		}
	}

	public static List<Object> getMetadata(JsonNode node) {
		ArrayList<Object> values = new ArrayList<Object>(Constants.metadataJsonAttributes.length);
		for (String attribute : Constants.metadataJsonAttributes) {
			String value = node.path(attribute).getTextValue();
			if (value == null) {
				value = "";
			}
			values.add(value);
		}
		return values;
	}
}
