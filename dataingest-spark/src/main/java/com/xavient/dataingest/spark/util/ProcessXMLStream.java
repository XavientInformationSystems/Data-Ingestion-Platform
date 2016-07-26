package com.xavient.dataingest.spark.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.xavient.dataingest.spark.constants.Constants;

public class ProcessXMLStream {

	public static final String complexElements = "book";
	public ArrayList<Object> xmlElements = new ArrayList<Object>(Constants.metadataXMLAttributes.length);
	public List<List<Object>> xmldata = new ArrayList<List<Object>>();
	public HashMap<String, String> complexList = new HashMap<String, String>();

	public ProcessXMLStream() {

		for (String s : complexElements.split(",")) {
			complexList.put(s, s);
		}

	}

	public List<List<Object>> getXMLData(String xmlStr) throws ParserConfigurationException, IOException {
		// Get Document Builder
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();

		// Build Document
		Document document;
		try {
			document = builder.parse(new InputSource(new StringReader(xmlStr)));

			// Normalize the XML Structure; It's just too important !!
			document.getDocumentElement().normalize();

			// Here comes the root node
			Element root = document.getDocumentElement();

			// Get all employees
			NodeList nList = document.getElementsByTagName("book");

			for (int temp = 0; temp < nList.getLength(); temp++) {
				xmlElements = new ArrayList<Object>(Constants.metadataXMLAttributes.length);
				xmldata.add(visitChildNodes(nList, temp));
			}
		} catch (SAXException e) {
			e.printStackTrace();
		}
		return xmldata;
	}

	public ArrayList<Object> visitChildNodes(NodeList nList, int temp) {

		Node node = nList.item(temp);
		if (node.getNodeType() == Node.ELEMENT_NODE)

		{
			if (complexList.get(node.getNodeName()) == null)
				this.xmlElements.add(node.getTextContent());

			if (node.hasAttributes()) {
				// get attributes names and values
				NamedNodeMap nodeMap = node.getAttributes();
				for (int i = 0; i < nodeMap.getLength(); i++) {
					Node tempNode = nodeMap.item(i);

					// this.xmlElements.add(tempNode.getTextContent() );
					this.xmlElements.add(tempNode.getNodeValue());

				}
			}
			if (node.hasChildNodes()) {
				for (int j = 0; j < node.getChildNodes().getLength(); j++)
					// We got more childs; Let's visit them as well
					visitChildNodes(node.getChildNodes(), j);

			}

		}
		return xmlElements;

	}
}