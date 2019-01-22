package com.xavient.dip.common.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xavient.dip.common.config.DiPConfiguration;
import com.xavient.dip.common.exceptions.DiPCommonException;

public class PropertyReader {

    static final Logger logger = LoggerFactory.getLogger(PropertyReader.class);

    private Properties properties;

    /**
     * Initializes and loads properties from file available at passed file path
     * @param filepath Properties file path
     * @throws DiPCommonException Throws exception if reading file fails
     */
    public PropertyReader(String filepath) throws DiPCommonException {
	InputStream is = null;
	// Check if filepath is null or empty
	if (StringUtils.isBlank(filepath)) {
	    // If path doesn't exists, try reading from classpath
	    is = this.getClass().getResourceAsStream(DiPConfiguration.DEFAULT_CONFIG_FILE);
	} else {
	    try {
		// If path exists, read the file
		is = new FileInputStream(new File(filepath));
	    } catch (FileNotFoundException e) {
		throw new DiPCommonException("File not found: {}", e);
	    }
	}
	loadProperties(is);
    }

    /**
     * Fetched the cached properties
     * @return Return the properties loaded from filepath
     */
    public Properties getProperties() {
	return properties;
    }

    private void loadProperties(InputStream is) throws DiPCommonException {
	logger.warn("Configuration file missing. Reading default properties file.");
	try {
	    properties = new Properties();
	    properties.load(is);
	} catch (FileNotFoundException e) {
	    throw new DiPCommonException("File not found: {}", e);
	} catch (IOException e) {
	    throw new DiPCommonException("Error while reading file: {}", e);
	} finally {
	    try {
		is.close();
	    } catch (IOException e) {
		logger.error("Error while closing properties file: {}", e);
	    }
	}
    }

    public Object getObjectProperty(String key) throws DiPCommonException {
	try {
	    return properties.getProperty(key);
	} catch (Exception e) {
	    throw new DiPCommonException(e);
	}
    }

    public String getProperty(String key) throws DiPCommonException {
	try {
	    return properties.getProperty(key);
	} catch (Exception e) {
	    throw new DiPCommonException(e);
	}
    }

    public String[] getSplitValueByDelim(String key, String delimiter) throws DiPCommonException {
	try {
	    return properties.get(key).toString().split(delimiter);
	} catch (Exception e) {
	    throw new DiPCommonException(e);
	}
    }

    public String[] getSplitValueByDelimKey(String key, String delimiterKey) throws DiPCommonException {
	try {
	    return properties.get(key).toString().split(properties.getProperty(delimiterKey));
	} catch (Exception e) {
	    throw new DiPCommonException(e);
	}
    }

    public int getIntProperty(String key) throws DiPCommonException {
	try {
	    return Integer.parseInt(properties.getProperty(key));
	} catch (NumberFormatException e) {
	    throw new DiPCommonException(e.getMessage());
	}
    }

    public double getDoubleProperty(String key) throws DiPCommonException {
	try {
	    return Double.parseDouble(properties.getProperty(key));
	} catch (NumberFormatException e) {
	    throw new DiPCommonException(e.getMessage());
	}
    }

    public float getFloatProperty(String key) throws DiPCommonException {
	try {
	    return Float.parseFloat(properties.getProperty(key));
	} catch (NumberFormatException e) {
	    throw new DiPCommonException(e.getMessage());
	}
    }

    public static void main(String[] args) throws DiPCommonException {
	PropertyReader prop = new PropertyReader("");
	for (String key : prop.getSplitValueByDelimKey("file.columns", "file.columns.delimiter")) {
	    logger.info(key);
	}
    }
}
