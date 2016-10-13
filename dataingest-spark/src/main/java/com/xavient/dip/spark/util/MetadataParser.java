package com.xavient.dip.spark.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.xavient.dip.spark.constants.Constants;
import com.xavient.dip.spark.exception.DataIngestException;

public class MetadataParser {

	private static final ObjectMapper m = new ObjectMapper();

	public static List<List<Object>> parse(String str) throws DataIngestException {
		List<List<Object>> objs = new ArrayList<>();
		try {
			if ((str.startsWith("{"))) {
				JsonNode rootNode = m.readTree(str);
				objs.add(getMetadata(rootNode));
			} else if ((str.startsWith("<"))) {
				objs = new ProcessXMLStream().getXMLData(str);
			} else {
				objs = new ProcessTSVStream().getTSVData(str);
			}

			return objs;
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
