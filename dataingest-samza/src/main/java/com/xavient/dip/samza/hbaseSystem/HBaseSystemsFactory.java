package com.xavient.dip.samza.hbaseSystem;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;

public class HBaseSystemsFactory implements SystemFactory{
	private static final Logger log = Logger.getLogger(HBaseSystemsFactory.class.getName());
	
	
	
	public SystemAdmin getAdmin(String systemName, Config config) {
		
		 return new  SinglePartitionWithoutOffsetsSystemAdmin();
	}

	public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {

		return null;
		

	}

	public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
	
	
			try {
				return   new HBaseSystemProducer(config);
			} catch (IOException e) {
				log.error("Error in creating HBaseSystemProducers : " + e.getMessage());
				e.printStackTrace();
				return null;
			}
		 
	}

}
