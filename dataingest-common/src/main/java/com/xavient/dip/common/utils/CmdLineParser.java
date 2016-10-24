/*
 * THis class is used t validate the command line arguments passed 
 * while invoking the process
 */

package com.xavient.dip.common.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xavient.dip.common.AppArgs;
import com.xavient.dip.common.config.DiPConfiguration;
import com.xavient.dip.common.exceptions.DataIngestException;


public class CmdLineParser implements Serializable {

	private static final long serialVersionUID = 1L;
	final static Logger logger = LoggerFactory.getLogger(CmdLineParser.class);

	private CommandLine getCommandLine(String[] args) throws DataIngestException {
		CommandLineParser parser = new BasicParser();
		CommandLine cmdLine;
		try {
			cmdLine = parser.parse(getOptions(), args);
		} catch (ParseException e) {
			getHelpStackTrace();
			throw new DataIngestException("Error while parsing the arguments: " + e.getMessage());
		}
		return cmdLine;
	}

	public AppArgs validateArgs(String[] args) throws DataIngestException {
		AppArgs appArgs = new AppArgs();
		CommandLine cmdLine = getCommandLine(args);
		parseAndValidateConfigFile(appArgs, cmdLine);
		return appArgs;
	}

	private void parseAndValidateConfigFile(AppArgs appArgs, CommandLine cmdLine) throws DataIngestException {
		if (cmdLine.hasOption(DiPConfiguration.REWIND)) {
			appArgs.setRewind(Boolean.valueOf(cmdLine.getOptionValue(DiPConfiguration.REWIND)));
		}
		if (!cmdLine.hasOption(DiPConfiguration.CONFIG_FILE)) {
			appArgs.setProperties(readFromClasspath());
		} else if (new File(cmdLine.getOptionValue(DiPConfiguration.CONFIG_FILE)).isDirectory()) {
			getHelpStackTrace();
			throw new DataIngestException(
					"Configuration file is a directory: " + cmdLine.getOptionValue(DiPConfiguration.CONFIG_FILE));
		} else if (!new File(cmdLine.getOptionValue(DiPConfiguration.CONFIG_FILE)).exists()) {
			getHelpStackTrace();
			throw new DataIngestException("Configuration file missing at: " + cmdLine.getOptionValue(DiPConfiguration.CONFIG_FILE));
		} else {
			Properties properties = new Properties();
			try {
				properties.load(new FileInputStream(new File(cmdLine.getOptionValue(DiPConfiguration.CONFIG_FILE))));
				appArgs.setProperties(properties);
			} catch (IOException e) {
				throw new DataIngestException("Error while loading configuration file: " + e.getMessage());
			}
		}
	}

	public Properties readFromClasspath() throws DataIngestException {
		logger.warn("Configuration file missing. Reading default properties file.");
		InputStream is = null;
		try {
			Properties properties = new Properties();
			is = this.getClass().getResourceAsStream(DiPConfiguration.DEFAULT_CONFIG_FILE);
			properties.load(is);
			return properties;
		} catch (FileNotFoundException e) {
			throw new DataIngestException("File not found: " + e.getLocalizedMessage());
		} catch (IOException e) {
			throw new DataIngestException("Error while reading file: " + e.getLocalizedMessage());
		} finally {
			try {
				is.close();
			} catch (IOException e) {
			}
		}
	}

	private Options getOptions() {
		Options options = new Options();
		options.addOption("c", DiPConfiguration.CONFIG_FILE, true, "Configuration file location");
		options.addOption("r", DiPConfiguration.REWIND, true, "(Optional) Whether to fetch the records from beginning");
		return options;
	}

	private void getHelpStackTrace() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("Storm-Topology", getOptions());
	}

}
