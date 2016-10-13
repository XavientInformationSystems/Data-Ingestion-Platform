/*
 * DataPayload contains an array list containing all the books present in the catalogue
 */

package com.xavient.dataingest.flink;

import java.util.List;
import java.io.Serializable;
import java.util.ArrayList;

public class DataPayload implements Serializable {

	private static final long serialVersionUID = 1L;
	public List<String> payload = new ArrayList<String>();

	/*
	 * This function is overwridden as this is used to write the data
	 * in  the HDFS. HDFS uses the toString function to write the data
	 */
	@Override
	public String toString() {

		String output = "";

		for (String s : payload) {

			output = output  + s.trim() + "\n";
		}
		return output.trim();
	}




	
}
