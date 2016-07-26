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

/*	public static void main(String[] args) throws ParserConfigurationException, IOException {

		ProcessXMLStream pxs = new ProcessXMLStream();
		List<List<Object>> dataCheck = pxs.getXMLData(
				"<catalog> <book id=\"bk101\">    <author>Gambardella, Matthew</author>    <title>XML Developer's Guide</title>    <genre>Computer</genre>    <price>44.95</price>    <publish_date>2000-10-01</publish_date>    <description>An in-depth look at creating applications with XML.</description>      </book> <book id=\"bk102\">    <author>Ralls, Kim</author>    <title>Midnight Rain</title>    <genre>Fantasy</genre>    <price>5.95</price>    <publish_date>2000-12-16</publish_date>    <description>A former architect battles corporate zombies, an evil sorceress, and her own childhood to become queen   of the world.</description> </book> <book id=\"bk103\">    <author>Corets, Eva</author>    <title>Maeve Ascendant</title>    <genre>Fantasy</genre>    <price>5.95</price>    <publish_date>2000-11-17</publish_date>    <description>After the collapse of a nanotechnology society in England, the young survivors lay the foundation for a new society.</description> </book> <book id=\"bk104\">    <author>Corets, Eva</author>    <title>Oberon's Legacy</title>    <genre>Fantasy</genre>    <price>5.95</price>    <publish_date>2001-03-10</publish_date>    <description>In post-apocalypse England, the mysterious agent known only as Oberon helps to create a new life for the inhabitants of London. Sequel to Maeve Ascendant.</description> </book> <book id=\"bk105\">    <author>Corets, Eva</author>    <title>The Sundered Grail</title>    <genre>Fantasy</genre>    <price>5.95</price>    <publish_date>2001-09-10</publish_date>    <description>The two daughters of Maeve, half-sisters, battle one another for control of England. Sequel to Oberon's Legacy.</description> </book> <book id=\"bk106\">    <author>Randall, Cynthia</author>    <title>Lover Birds</title>    <genre>Romance</genre>    <price>4.95</price>    <publish_date>2000-09-02</publish_date>    <description>When Carla meets Paul at an ornithology conference, tempers fly as feathers get ruffled.</description> </book> <book id=\"bk107\">    <author>Thurman, Paula</author>    <title>Splish Splash</title>    <genre>Romance</genre>    <price>4.95</price>    <publish_date>2000-11-02</publish_date>    <description>A deep sea diver finds true love twenty thousand leagues beneath the sea.</description> </book> <book id=\"bk108\">    <author>Knorr, Stefan</author>    <title>Creepy Crawlies</title>    <genre>Horror</genre>    <price>4.95</price>    <publish_date>2000-12-06</publish_date>    <description>An anthology of horror stories about roaches, centipedes, scorpions  and other insects.</description> </book> <book id=\"bk109\">    <author>Kress, Peter</author>    <title>Paradox Lost</title>    <genre>Science Fiction</genre>    <price>6.95</price>    <publish_date>2000-11-02</publish_date>    <description>After an inadvertant trip through a Heisenberg Uncertainty Device, James Salway discovers the problems of being quantum.</description> </book> <book id=\"bk110\">    <author>O'Brien, Tim</author>    <title>Microsoft .NET: The Programming Bible</title>    <genre>Computer</genre>    <price>36.95</price>    <publish_date>2000-12-09</publish_date>    <description>Microsoft's .NET initiative is explored in detail in this deep programmer's reference.</description> </book> <book id=\"bk111\">    <author>O'Brien, Tim</author>    <title>MSXML3: A Comprehensive Guide</title>    <genre>Computer</genre>    <price>36.95</price>    <publish_date>2000-12-01</publish_date>    <description>The Microsoft MSXML3 parser is covered in  detail, with attention to XML DOM interfaces, XSLT processing, SAX and more.</description> </book> <book id=\"bk112\">    <author>Galos, Mike</author>    <title>Visual Studio 7: A Comprehensive Guide</title>    <genre>Computer</genre>    <price>49.95</price>    <publish_date>2001-04-16</publish_date>    <description>Microsoft Visual Studio 7 is explored in depth, looking at how Visual Basic, Visual C++, C#, and ASP+ are integrated into a comprehensive development environment.</description> </book></catalog>");
		// List<List<Object>> dataCheck =
		// pxs.getXMLData("<books><author>Gambardella,
		// Matthew</author></books>");

		for (List<Object> nodes : dataCheck) {
			System.out.println();
			for (Object o : nodes) {
				System.out.print(o.toString() + " | ");
			}

		}

	}*/

}
