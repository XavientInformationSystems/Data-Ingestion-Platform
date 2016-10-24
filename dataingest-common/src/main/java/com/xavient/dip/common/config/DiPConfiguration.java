package com.xavient.dip.common.config;

public class DiPConfiguration {
	public static final String CONFIG_FILE = "config";
	public static final String DEFAULT_CONFIG_FILE = "/dip-default-config.properties";
	
	public static final String REWIND = "rewind";
	public static final String KAFKA_TOPIC = "kafka.topic";
	public static final String KAFKA_GROUP_ID="group.id";
	public static final String KAFKA_BOOTSTRAP_SERVERS="bootstrap.servers";
	public static final String ZK_HOST = "zookeeper.host";
	public static final String ZK_PORT = "zookeeper.port";
	
	public static final String HBASE_CONFIG_FILE = "hbase.config.file";
	public static final String HBASE_MASTER = "hbase.master.port";
	public static final String HBASE_TABLE_NAME ="hbase.table.name";
	public static final String HBASE_ROW_KEY_CHECK = "hbase.row.key.check";
	public static final String HBASE_ROW_KEY= "hbase.row.key";
	public static final String HBASE_COL_FAMILY = "hbase.col.families";
	public static final String HBASE_COL_NAMES = "hbase.col.names";
	public static final String HBASE_COL_DELIMITER = "hbase.col.delimiter";
	public static final String HBASE_COL_FAM_DELIMITER = "hbase.col.families.delimiter";
	
	public static final String DELIMITER_PREFIX = "\\";
	
	public static final String HADOOP_USER_NAME = "hadoop.user.name";
	public static final String HDFS_FILE_NAME = "hdfs.file.name";
	public static final String FILE_SIZE_ROTATION = "rotation.policy.file.size";
	public static final String HDFS_OUTPUT_PATH = "hdfs.output.path";
	public static final String HDFS_OUTPUT_DELIMITER = "hdfs.output.delimiter";
	public static final String HDFS_SYNC_POLICY = "hdfs.sync.policy";
	public static final String CLUSTER_FS_URL = "cluster.fs.url";
	
	//JDBC 
	public static final String JDBC_URL="jdbc.url";
	public static final String JDBC_USER="jdbc.user";
	public static final String JDBC_PASSWORD="jdbc.password";
	public static final String JDBC_DRIVER_CLASS="jdbc.driver.class";
	
	public static final String FILE_DELIMITER = "|";
	public static final String[] METADATA_JSON_ATTRIBUTES = { "id", "author", "title", "genre", "price", "publish_date",
			"description" };

}
