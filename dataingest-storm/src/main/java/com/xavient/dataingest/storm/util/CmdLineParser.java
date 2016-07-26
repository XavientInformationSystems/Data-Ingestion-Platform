package com.xavient.dataingest.storm.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xavient.dataingest.storm.constants.Constants;
import com.xavient.dataingest.storm.exception.DataIngestException;
import com.xavient.dataingest.storm.vo.AppArgs;

public class CmdLineParser {

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
		if (cmdLine.hasOption(Constants.REWIND)) {
			appArgs.setRewind(Boolean.valueOf(cmdLine.getOptionValue(Constants.REWIND)));
		}
		if (!cmdLine.hasOption(Constants.CONFIG)) {
			appArgs.setProperties(readFromClasspath());
		} else if (new File(cmdLine.getOptionValue(Constants.CONFIG)).isDirectory()) {
			getHelpStackTrace();
			throw new DataIngestException(
					"Configuration file is a directory: " + cmdLine.getOptionValue(Constants.CONFIG));
		} else if (!new File(cmdLine.getOptionValue(Constants.CONFIG)).exists()) {
			getHelpStackTrace();
			throw new DataIngestException("Configuration file missing at: " + cmdLine.getOptionValue(Constants.CONFIG));
		} else {
			Properties properties = new Properties();
			try {
				properties.load(new FileInputStream(new File(cmdLine.getOptionValue(Constants.CONFIG))));
				appArgs.setProperties(properties);
			} catch (IOException e) {
				throw new DataIngestException("Error while loading configuration file: " + e.getMessage());
			}
		}
	}

	private Properties readFromClasspath() throws DataIngestException {
		System.out.println("Configuration file missing. Reading default properties file.");
		logger.warn("Configuration file missing. Reading default properties file.");
		InputStream is = null;
		try {
			Properties properties = new Properties();
			is = this.getClass().getResourceAsStream("/config.properties");
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
		options.addOption("c", Constants.CONFIG, true, "Configuration file location");
		options.addOption("r", Constants.REWIND, true, "(Optional) Whether to fetch the records from beginning");
		return options;
	}

	private void getHelpStackTrace() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("Storm-Topology", getOptions());
	}

}
