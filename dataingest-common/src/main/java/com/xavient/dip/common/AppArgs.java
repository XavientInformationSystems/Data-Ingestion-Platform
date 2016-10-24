/*
 * This class is used to load the properties from the file entered through 
 * command line  or from the default configurations
 */
package com.xavient.dip.common;

import java.io.Serializable;
import java.util.Properties;

public class AppArgs implements Serializable {

	private static final long serialVersionUID = 6792599624479386357L;

	private boolean rewind;
	private Properties properties;

	public boolean isRewind() {
		return rewind;
	}

	public void setRewind(boolean rewind) {
		this.rewind = rewind;
	}

	public Properties getProperties() {
		return properties;
	}
	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public String getProperty(String key) {
		return properties.getProperty(key);
	}

	public String getProperty(String key, String defaultVal) {
		return properties.getProperty(key, defaultVal);
	}


}
