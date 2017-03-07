package com.xavient.dip.samza.Utils;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import org.apache.samza.config.Config;

public class ApplicationProperties implements Serializable {

	private static final long serialVersionUID = -2130249690414270259L;

	public Properties getProp() {
		return prop;
	}

	public void setProp(Properties prop) {
		this.prop = prop;
	}

	Properties prop = new Properties();

	public ApplicationProperties(Config config) throws IOException {

		config.forEach((x, y) -> prop.put((String) x, (String) y));

	}

}
