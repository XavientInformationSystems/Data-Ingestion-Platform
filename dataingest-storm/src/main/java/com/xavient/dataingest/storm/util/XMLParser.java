package com.xavient.dataingest.storm.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.xavient.dataingest.storm.exception.DataIngestException;
import com.xavient.dataingest.storm.vo.Catalog;

public class XMLParser {

	public static Catalog parseString(String line) throws DataIngestException {
		return null;
		/*
		 * JacksonXmlModule module = new JacksonXmlModule();
		 * module.setDefaultUseWrapper(false); XmlMapper mapper = new
		 * XmlMapper(module); try { return mapper.readValue(line,
		 * Catalog.class); } catch (IOException e) { throw new
		 * DataIngestException(e); }
		 */
	}

	public static void main(String[] args) throws DataIngestException {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(new File("src/main/resources/data.xml")));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		String s = null;
		StringBuilder xmlContent = new StringBuilder();
		try {
			while ((s = br.readLine()) != null) {
				xmlContent.append(s);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(xmlContent);
		Catalog c = parseString(xmlContent.toString());
		System.out.println("--" + c);
	}
}
